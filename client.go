package betcd

import (
	"context"
	"time"

	"github.com/grpc-boot/base"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/client/v3"
)

// KV etcd KV
type KV interface {
	// LoadKey 加载前缀到缓存
	LoadKey(prefix string, opts ...clientv3.OpOption) (err error)
	// Watch 监视某个Key，返回WatchChan
	Watch(ctx context.Context, key string, opts ...clientv3.OpOption) (wch clientv3.WatchChan)
	// WatchKey 监视某个Key，并修改cache
	WatchKey(key string, opts ...clientv3.OpOption)
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
	// Connection 获取ectd conf
	Connection() (client *clientv3.Client)
	// Close ---
	Close() (err error)
}

func NewClient(v3Conf *clientv3.Config, prefixList []string, keyOptions map[string]KeyOption, watchOptions ...clientv3.OpOption) (c KV, err error) {
	var conn *clientv3.Client
	conn, err = clientv3.New(*v3Conf)

	if err != nil {
		return nil, err
	}

	c = &conf{
		connection: conn,
		cache:      base.NewShardMap(),
		decoder:    NewDeserializer(keyOptions),
	}

	if len(prefixList) > 0 {
		for _, prefix := range prefixList {
			//加载配置
			if er := c.LoadKey(prefix, clientv3.WithPrefix()); er != nil {
				//记录错误并返回，不中断加载
				err = er
			}

			go func(p string) {
				c.WatchKey(p, watchOptions...)
			}(prefix)
		}
	}

	return c, err
}

type conf struct {
	connection *clientv3.Client
	cache      base.ShardMap
	decoder    Deserializer
}

func (c *conf) Watch(ctx context.Context, key string, opts ...clientv3.OpOption) (wch clientv3.WatchChan) {
	return c.connection.Watch(ctx, key, opts...)
}

func (c *conf) LoadKey(prefix string, opts ...clientv3.OpOption) (err error) {
	resp, err := c.connection.Get(context.Background(), prefix, opts...)
	if err != nil {
		return err
	}

	for _, ev := range resp.Kvs {
		key := base.Bytes2String(ev.Key)
		if value, er := c.decoder.Deserialize(key, ev.Value); er == nil {
			c.cache.Set(key, value)
		} else {
			//解析出现错误，记录并返回错误，不影响其他配置加载
			err = er
		}
	}

	return err
}

func (c *conf) WatchKey(key string, opts ...clientv3.OpOption) {
	watchanel := c.Watch(context.Background(), key, opts...)

	for watchResponse := range watchanel {
		for _, ev := range watchResponse.Events {
			switch ev.Type {
			case mvccpb.PUT:
				k := string(ev.Kv.Key)
				if value, err := c.decoder.Deserialize(k, ev.Kv.Value); err == nil {
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
	resp, err = c.connection.Get(ctx, key)
	if err != nil {
		return
	}
	return resp.Kvs, nil
}

func (c *conf) GetRemoteContext(ctx context.Context, key string) (kvs []*mvccpb.KeyValue, err error) {
	var resp *clientv3.GetResponse
	resp, err = c.connection.Get(ctx, key)
	if err != nil {
		return
	}
	return resp.Kvs, nil
}

func (c *conf) Put(key string, value string, timeout time.Duration, opts ...clientv3.OpOption) (resp *clientv3.PutResponse, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return c.connection.Put(ctx, key, value, opts...)
}

func (c *conf) PutContext(ctx context.Context, key string, value string, opts ...clientv3.OpOption) (resp *clientv3.PutResponse, err error) {
	return c.connection.Put(ctx, key, value, opts...)
}

func (c *conf) Delete(key string, timeout time.Duration, opts ...clientv3.OpOption) (resp *clientv3.DeleteResponse, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return c.connection.Delete(ctx, key, opts...)
}

func (c *conf) Connection() (client *clientv3.Client) {
	return c.connection
}

func (c *conf) Close() (err error) {
	return c.connection.Close()
}
