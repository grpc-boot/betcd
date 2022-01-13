package betcd

import (
	"context"
	"time"

	"github.com/grpc-boot/base"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/client/v3"
)

// Kv etcd Kv
type Kv interface {
	// LoadKey 加载前缀到缓存
	LoadKey(prefix string, opts ...clientv3.OpOption) (err error)
	// Watch 监视某个Key，返回WatchChan
	Watch(ctx context.Context, key string, opts ...clientv3.OpOption) (wch clientv3.WatchChan)
	// WatchKey4Cache 监视某个Key，并修改cache
	WatchKey4Cache(key string, opts ...clientv3.OpOption)
	// Put 修改某个Key值
	Put(key string, value string, timeout time.Duration, opts ...clientv3.OpOption) (resp *clientv3.PutResponse, err error)
	// PutContext with context修改某个Key值
	PutContext(ctx context.Context, key string, value string, opts ...clientv3.OpOption) (resp *clientv3.PutResponse, err error)
	// Get 获取数据
	Get(key string) (value interface{}, exists bool)
	// GetRemote 从etcd远程获取数据
	GetRemote(key string, timeout time.Duration) (kvs []*mvccpb.KeyValue, err error)
	// GetRemoteContext with context从etcd远程获取数据
	GetRemoteContext(ctx context.Context, key string) (kvs []*mvccpb.KeyValue, err error)
	// Delete 删除某个Key数据
	Delete(key string, timeout time.Duration, opts ...clientv3.OpOption) (resp *clientv3.DeleteResponse, err error)
	// Connection 获取ectd keyValue
	Connection() (client *clientv3.Client)
	// Close ---
	Close() (err error)
}

func NewKv(v3Conf *clientv3.Config, prefixList []string, keyOptions map[string]KeyOption, watchOptions ...clientv3.OpOption) (c Kv, err error) {
	var conn *clientv3.Client
	conn, err = clientv3.New(*v3Conf)

	if err != nil {
		return nil, err
	}

	c = &keyValue{
		client:  conn,
		cache:   base.NewShardMap(),
		decoder: NewDeserializer(keyOptions),
	}

	if len(prefixList) > 0 {
		for _, prefix := range prefixList {
			//加载配置
			if er := c.LoadKey(prefix, clientv3.WithPrefix()); er != nil {
				//记录错误并返回，不中断加载
				err = er
			}

			go func(p string) {
				c.WatchKey4Cache(p, watchOptions...)
			}(prefix)
		}
	}

	return c, err
}

type keyValue struct {
	client  *clientv3.Client
	cache   base.ShardMap
	decoder Deserializer
}

func (kv *keyValue) LoadKey(prefix string, opts ...clientv3.OpOption) (err error) {
	resp, err := kv.client.Get(context.Background(), prefix, opts...)
	if err != nil {
		return err
	}

	for _, ev := range resp.Kvs {
		key := base.Bytes2String(ev.Key)
		if value, er := kv.decoder.Deserialize(key, ev.Value); er == nil {
			kv.cache.Set(key, value)
		} else {
			//解析出现错误，记录并返回错误，不影响其他配置加载
			err = er
		}
	}

	return err
}

func (kv *keyValue) Watch(ctx context.Context, key string, opts ...clientv3.OpOption) (wch clientv3.WatchChan) {
	return kv.client.Watch(ctx, key, opts...)
}

func (kv *keyValue) WatchKey4Cache(key string, opts ...clientv3.OpOption) {
	watchChan := kv.Watch(context.Background(), key, opts...)
	go func() {
		for watchResponse := range watchChan {
			for _, ev := range watchResponse.Events {
				switch ev.Type {
				case mvccpb.PUT:
					k := string(ev.Kv.Key)
					if value, err := kv.decoder.Deserialize(k, ev.Kv.Value); err == nil {
						kv.cache.Set(k, value)
					}
				case mvccpb.DELETE:
					kv.cache.Delete(ev.Kv)
				}
			}
		}
	}()
}

func (kv *keyValue) Get(key string) (value interface{}, exists bool) {
	return kv.cache.Get(key)
}

func (kv *keyValue) GetRemote(key string, timeout time.Duration) (kvs []*mvccpb.KeyValue, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	var resp *clientv3.GetResponse
	resp, err = kv.client.Get(ctx, key)
	if err != nil {
		return
	}
	return resp.Kvs, nil
}

func (kv *keyValue) GetRemoteContext(ctx context.Context, key string) (kvs []*mvccpb.KeyValue, err error) {
	var resp *clientv3.GetResponse
	resp, err = kv.client.Get(ctx, key)
	if err != nil {
		return
	}
	return resp.Kvs, nil
}

func (kv *keyValue) Put(key string, value string, timeout time.Duration, opts ...clientv3.OpOption) (resp *clientv3.PutResponse, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return kv.client.Put(ctx, key, value, opts...)
}

func (kv *keyValue) PutContext(ctx context.Context, key string, value string, opts ...clientv3.OpOption) (resp *clientv3.PutResponse, err error) {
	return kv.client.Put(ctx, key, value, opts...)
}

func (kv *keyValue) Delete(key string, timeout time.Duration, opts ...clientv3.OpOption) (resp *clientv3.DeleteResponse, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return kv.client.Delete(ctx, key, opts...)
}

func (kv *keyValue) Connection() (client *clientv3.Client) {
	return kv.client
}

func (kv *keyValue) Close() (err error) {
	return kv.client.Close()
}
