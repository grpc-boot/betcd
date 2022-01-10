package betcd

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/conf/v3"
)

type Service interface {
	// Register 注册服务
	Register(serviceTarget string, value string)
	// Get 获取所有服务列表
	Get(serviceTarget string) (list map[string]interface{}, exists bool)
	// RandOne 随机取一个服务
	RandOne(key string) (value interface{}, exists bool)
	// Range 遍历服务，handler返回true时停止遍历
	Range(key string, handler func(index string, val interface{}) (handled bool))
	Close() (err error)
}

func NewService(conf *clientv3.Config, prefix string, deserializers map[string]Deserialize, opts ...clientv3.OpOption) (s Service, err error) {
	var serv *service
	serv, err = newService(conf, deserializers)
	if err != nil {
		return
	}
	serv.prefix = prefix

	var resp *clientv3.GetResponse

	resp, err = serv.connection.Get(context.Background(), serv.prefix, clientv3.WithPrefix())
	if err != nil {
		return
	}

	serv.mutex.Lock()
	defer serv.mutex.Unlock()

	for _, ev := range resp.Kvs {
		serviceTarget, index := serv.key2Target(string(ev.Key))
		if serviceTarget == "" || index == "" {
			continue
		}

		value, er := deserialize(&deserializers, serviceTarget, ev.Value)
		if er != nil {
			continue
		}

		if _, exists := serv.service[serviceTarget]; !exists {
			serv.service[serviceTarget] = make(map[string]interface{})
		}

		serv.service[serviceTarget][index] = value
	}

	serv.watch(opts...)

	return serv, err
}

type service struct {
	mutex         sync.RWMutex
	prefix        string
	service       map[string]map[string]interface{}
	connection    *clientv3.Client
	deserializers Deserializer
}

func newService(conf *clientv3.Config, keyOptions map[string]KeyOption) (c *service, err error) {
	var conn *clientv3.Client
	conn, err = clientv3.New(*conf)

	if err != nil {
		return nil, err
	}

	c = &service{
		connection:    conn,
		service:       make(map[string]map[string]interface{}),
		deserializers: NewDeserializer(keyOptions),
	}
	return
}

func (s *service) key2Target(key string) (serviceTarget, index string) {
	if (len(s.prefix) + 4) > len(key) {
		return
	}

	if strings.Index(key, s.prefix) != 0 {
		return
	}

	serviceSuffix := key[len(s.prefix)+1:]
	keyEnd := strings.Index(serviceSuffix, "/")
	if keyEnd < 1 || keyEnd == len(serviceSuffix) {
		return
	}

	serviceTarget = serviceSuffix[:keyEnd]
	index = serviceSuffix[keyEnd+1:]
	return
}

func (s *service) watch(opts ...clientv3.OpOption) {
	go func() {
		watchanel := s.connection.Watch(context.Background(), s.prefix, opts...)
		for watchResponse := range watchanel {
			for _, ev := range watchResponse.Events {
				switch ev.Type {
				case mvccpb.PUT:
					s.addService(string(ev.Kv.Key), ev.Kv.Value)
				case mvccpb.DELETE:
					s.delService(string(ev.Kv.Key))
				}
			}
		}
	}()
}

func (s *service) addService(key string, value interface{}) {
	serviceTarget, index := s.key2Target(key)
	if serviceTarget == "" || index == "" {
		return
	}

	val, err := s.deserializers.Deserialize(serviceTarget, value.([]byte))
	if err != nil {
		return
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()
	if _, exists := s.service[serviceTarget]; !exists {
		s.service[serviceTarget] = make(map[string]interface{})
	}
	s.service[serviceTarget][index] = val
}

func (s *service) delService(key string) {
	serviceTarget, index := s.key2Target(key)
	if serviceTarget == "" || index == "" {
		return
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	if _, exists := s.service[serviceTarget]; !exists {
		return
	}

	delete(s.service[serviceTarget], index)
}

func (s *service) Register(serviceTarget string, value string) {
	serviceTarget = fmt.Sprintf(s.prefix+"/%s/", serviceTarget)
	var (
		kv                          = clientv3.NewKV(s.connection)
		lease                       = clientv3.NewLease(s.connection)
		curLeaseId clientv3.LeaseID = 0
		leaseResp  *clientv3.LeaseGrantResponse
		err        error
	)

	go func() {
		defer func() {
			if curLeaseId != 0 {
				_, _ = lease.Revoke(context.TODO(), curLeaseId)
			}
		}()

		tick := time.NewTicker(time.Second)
		for range tick.C {
			if curLeaseId == 0 {
				leaseResp, err = lease.Grant(context.TODO(), 10)
				if err != nil {
					continue
				}

				key := serviceTarget + fmt.Sprintf("%d", leaseResp.ID)
				if _, err = kv.Put(context.TODO(), key, value, clientv3.WithLease(leaseResp.ID)); err != nil {
					continue
				}
				curLeaseId = leaseResp.ID
			} else {
				if _, err = lease.KeepAliveOnce(context.TODO(), curLeaseId); err == rpctypes.ErrLeaseNotFound {
					curLeaseId = 0
					continue
				}
			}
		}
	}()
}

func (s *service) Get(key string) (list map[string]interface{}, exists bool) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	var items map[string]interface{}
	items, exists = s.service[key]
	if exists {
		//拷贝，否则有并发问题
		list = make(map[string]interface{}, len(items))
		for index, value := range list {
			list[index] = value
		}
	}
	return
}

func (s *service) RandOne(key string) (value interface{}, exists bool) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	var items map[string]interface{}
	items, exists = s.service[key]
	if exists {
		for _, val := range items {
			return val, exists
		}
	}
	return
}

func (s *service) Range(key string, handler func(index string, val interface{}) (handled bool)) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	items, exists := s.service[key]
	if exists {
		for index, val := range items {
			if handler(index, val) {
				return
			}
		}
	}
}

func (s *service) Close() (err error) {
	return s.connection.Close()
}
