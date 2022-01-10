package betcd

import (
	"context"
	"time"

	"github.com/grpc-boot/base"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type Conf interface {
	// Watch 监视某个Key
	Watch(key string, opts ...clientv3.OpOption)
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
}

func NewConf(conf *clientv3.Config, keyOptions map[string]KeyOption) (cf Conf, err error) {
	cli, err := NewClient(conf)
	if err != nil {
		return nil, err
	}

	return NewConfWithClient(cli, keyOptions), nil
}

func NewConfWithClient(client Client, keyOptions map[string]KeyOption) (cf Conf) {
	cf = &conf{
		client:     client,
		cache:      base.NewShardMap(),
		keyOptions: keyOptions,
	}

	return cf
}

type conf struct {
	client     Client
	cache      base.ShardMap
	keyOptions map[string]KeyOption
}

func (c *conf) deserialize(key string, val []byte) (value interface{}, err error) {
	if c.keyOptions == nil {
		return val, err
	}

	if option, exists := c.keyOptions[key]; exists {
		//优先使用自定义解析
		if option.Deserialize != nil {
			return option.Deserialize(val)
		}

		switch option.Type {
		case Json:
			return JsonMapDeserialize(val)
		case Yaml:
			return YamlMapDeserialize(val)
		case String:
			return StringDeserialize(val)
		}
	}

	return val, err
}

func (c *conf) Watch(key string, opts ...clientv3.OpOption) {
	watchanel := c.client.Connection().Watch(context.Background(), key, opts...)
	for watchResponse := range watchanel {
		for _, ev := range watchResponse.Events {
			switch ev.Type {
			case mvccpb.PUT:
				k := string(ev.Kv.Key)
				if value, err := c.deserialize(k, ev.Kv.Value); err == nil {
					c.cache.Set(k, value)
				}
			case mvccpb.DELETE:
				c.cache.Delete(ev.Kv)
			}
		}
	}
}

func (c *conf) Get(key string) (value interface{}, exists bool) {
	return c.cache.Get(key)
}

func (c *conf) GetRemote(key string, timeout time.Duration) (kvs []*mvccpb.KeyValue, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	var resp *clientv3.GetResponse
	resp, err = c.client.Connection().Get(ctx, key)
	if err != nil {
		return
	}
	return resp.Kvs, nil
}

func (c *conf) GetRemoteContext(ctx context.Context, key string) (kvs []*mvccpb.KeyValue, err error) {
	var resp *clientv3.GetResponse
	resp, err = c.client.Connection().Get(ctx, key)
	if err != nil {
		return
	}
	return resp.Kvs, nil
}

func (c *conf) Put(key string, value string, timeout time.Duration, opts ...clientv3.OpOption) (resp *clientv3.PutResponse, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return c.client.Connection().Put(ctx, key, value, opts...)
}

func (c *conf) PutContext(ctx context.Context, key string, value string, opts ...clientv3.OpOption) (resp *clientv3.PutResponse, err error) {
	return c.client.Connection().Put(ctx, key, value, opts...)
}

func (c *conf) Delete(key string, timeout time.Duration, opts ...clientv3.OpOption) (resp *clientv3.DeleteResponse, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return c.client.Connection().Delete(ctx, key, opts...)
}
