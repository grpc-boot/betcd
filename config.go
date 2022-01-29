package betcd

import (
	"context"
	"sync"
	"time"

	"github.com/grpc-boot/base"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/client/v3"
)

// Config etcd Config
type Config interface {
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
	// Connection 获取ectd config
	Connection() (client *clientv3.Client)
	// Close ---
	Close() (err error)
}

func NewConfig(v3Conf *clientv3.Config, prefixList []string, keyOptions []KeyOption, watchOptions ...clientv3.OpOption) (c Config, err error) {
	var client *clientv3.Client
	client, err = clientv3.New(*v3Conf)

	if err != nil {
		return nil, err
	}

	return NewConfigWithClient(client, prefixList, keyOptions, watchOptions...)
}

func NewConfigWithClient(client *clientv3.Client, prefixList []string, keyOptions []KeyOption, watchOptions ...clientv3.OpOption) (c Config, err error) {
	var kom map[string]KeyOption
	if len(keyOptions) > 0 {
		kom = make(map[string]KeyOption, len(keyOptions))
		for _, option := range keyOptions {
			kom[option.Key] = option
		}
	}

	c = &config{
		client:    client,
		cache:     base.NewShardMap(),
		decoder:   NewDeserializer(kom),
		cacheChan: make(map[string]chan struct{}, len(prefixList)),
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

// EventHandler 事件处理
type EventHandler func(et mvccpb.Event_EventType, key string, value interface{})

type config struct {
	client      *clientv3.Client
	cache       base.ShardMap
	decoder     Deserializer
	mutex       sync.RWMutex
	hasClose    bool
	cacheChan   map[string]chan struct{}
	changeEvent EventHandler
}

func (c *config) LoadKey(prefix string, opts ...clientv3.OpOption) (err error) {
	resp, err := c.client.Get(context.TODO(), prefix, opts...)
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

func (c *config) Watch(ctx context.Context, key string, opts ...clientv3.OpOption) (wch clientv3.WatchChan) {
	return c.client.Watch(ctx, key, opts...)
}

func (c *config) WatchKey4Cache(key string, opts ...clientv3.OpOption) {
	var (
		watchChan = c.Watch(context.TODO(), key, opts...)
		done      = make(chan struct{}, 1)
	)

	c.mutex.Lock()
	c.cacheChan[key] = done
	c.mutex.Unlock()

	for {
		select {
		case <-done:
			return
		case watchResponse, ok := <-watchChan:
			if len(watchResponse.Events) > 0 {
				for _, ev := range watchResponse.Events {
					switch ev.Type {
					case mvccpb.PUT:
						k := base.Bytes2String(ev.Kv.Key)
						if value, err := c.decoder.Deserialize(k, ev.Kv.Value); err == nil {
							c.cache.Set(k, value)
						}
					case mvccpb.DELETE:
						c.cache.Delete(base.Bytes2String(ev.Kv.Key))
					}
				}
			}

			if !ok {
				return
			}
		}
	}
}

func (c *config) Get(key string) (value interface{}, exists bool) {
	return c.cache.Get(key)
}

func (c *config) GetRemote(key string, timeout time.Duration) (kvs []*mvccpb.KeyValue, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	var resp *clientv3.GetResponse
	resp, err = c.client.Get(ctx, key)
	if err != nil {
		return
	}
	return resp.Kvs, nil
}

func (c *config) GetRemoteContext(ctx context.Context, key string) (kvs []*mvccpb.KeyValue, err error) {
	var resp *clientv3.GetResponse
	resp, err = c.client.Get(ctx, key)
	if err != nil {
		return
	}
	return resp.Kvs, nil
}

func (c *config) Put(key string, value string, timeout time.Duration, opts ...clientv3.OpOption) (resp *clientv3.PutResponse, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return c.client.Put(ctx, key, value, opts...)
}

func (c *config) PutContext(ctx context.Context, key string, value string, opts ...clientv3.OpOption) (resp *clientv3.PutResponse, err error) {
	return c.client.Put(ctx, key, value, opts...)
}

func (c *config) Delete(key string, timeout time.Duration, opts ...clientv3.OpOption) (resp *clientv3.DeleteResponse, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return c.client.Delete(ctx, key, opts...)
}

func (c *config) Connection() (client *clientv3.Client) {
	return c.client
}

func (c *config) Close() (err error) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	if c.hasClose {
		return
	}

	for _, ch := range c.cacheChan {
		ch <- base.SetValue
	}

	c.hasClose = true
	return c.client.Close()
}
