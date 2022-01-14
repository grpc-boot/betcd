# betcd

### 配置中心

> 代码

```go
package main

import (
	"encoding/xml"
	"time"

	"github.com/grpc-boot/base"
	"github.com/grpc-boot/betcd"
	clientv3 "go.etcd.io/etcd/client/v3"
)

var (
	etcdClient *clientv3.Client
	config     betcd.Config
)

const (
	ConfVersion = `browser/conf/app/version`
	ConfLimit   = `browser/conf/app/limit`
	ConfRedis   = `browser/conf/app/redis`
	ConfMysql   = `browser/conf/app/mysql`
)

var (
	keyOptions = []betcd.KeyOption{
		{Key: ConfVersion, Type: betcd.String},
		{Key: ConfLimit, Type: betcd.Json},
		{Key: ConfRedis, Type: betcd.Yaml},
		{Key: ConfMysql, Deserialize: func(data []byte) (value interface{}, err error) {
			var val MysqlOption
			err = xml.Unmarshal(data, &val)
			return val, err
		}},
	}
)

type MysqlOption struct {
	Host     string `xml:"host"`
	Port     uint16 `xml:"port"`
	DbName   string `xml:"dbname"`
	UserName string `xml:"userName"`
	Password string `xml:"password"`
}

func init() {
	var err error

	etcdClient, err = clientv3.New(clientv3.Config{
		Endpoints:            []string{"127.0.0.1:2379"},
		DialTimeout:          time.Second,
		DialKeepAliveTime:    time.Second * 100,
		DialKeepAliveTimeout: time.Second * 10,
	})

	if err != nil {
		base.RedFatal("init etcd err:%s", err.Error())
	}

	config, err = betcd.NewConfigWithClient(etcdClient, []string{"browser/conf/app"}, keyOptions, clientv3.WithPrefix())
	if err != nil {
		base.RedFatal("init etcd config err:%s", err.Error())
	}
}

func main() {
	go logging()

	_, err := config.Put(ConfVersion, "12.0.0.3", time.Second)
	if err != nil {
		base.Red("put %s err:%s", ConfVersion, err.Error())
	}

	limitConf := map[string]interface{}{
		"/user/info": map[string]interface{}{
			"limit": 5000,
		},
		"/user/login": map[string]interface{}{
			"limit": 500,
		},
	}

	data, _ := base.JsonEncode(limitConf)
	_, err = config.Put(ConfLimit, base.Bytes2String(data), time.Second)
	if err != nil {
		base.Red("put %s err:%s", ConfLimit, err.Error())
	}

	redisConf := map[string]interface{}{
		"host": "127.0.0.1",
		"port": 6378,
		"auth": "ab2343sadfa23",
	}
	data, _ = base.YamlEncode(redisConf)
	_, err = config.Put(ConfRedis, base.Bytes2String(data), time.Second)
	if err != nil {
		base.Red("put %s err:%s", ConfRedis, err.Error())
	}

	mysqlConf := MysqlOption{
		Host:     "127.0.0.1",
		Port:     3309,
		DbName:   "test",
		UserName: "test",
		Password: "12345678",
	}
	data, _ = xml.Marshal(mysqlConf)
	_, err = config.Put(ConfMysql, base.Bytes2String(data), time.Second)
	if err != nil {
		base.Red("put %s err:%s", ConfMysql, err.Error())
	}

	defer config.Close()

	var done chan struct{}
	<-done
}

func logging() {
	tick := time.NewTicker(time.Second)
	for range tick.C {
		val, exists := config.Get(ConfVersion)
		base.Green("%s--%+v--%t", ConfVersion, val, exists)

		val, exists = config.Get(ConfLimit)
		base.Green("%s--%+v--%t", ConfLimit, val, exists)

		val, exists = config.Get(ConfRedis)
		base.Green("%s--%+v--%t", ConfRedis, val, exists)

		val, exists = config.Get(ConfMysql)
		base.Green("%s--%+v--%t", ConfMysql, val, exists)
	}
}

```

### 服务注册与发现

```go
package main

import (
	"context"
	"time"

	"github.com/grpc-boot/base"
	"github.com/grpc-boot/betcd"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/naming/endpoints"
)

var (
	etcdClient *clientv3.Client
	naming     betcd.Naming
)

const (
	ServiceApp = `browser/services/app`
)

func init() {
	var err error

	etcdClient, err = clientv3.New(clientv3.Config{
		Endpoints:            []string{"127.0.0.1:2379"},
		DialTimeout:          time.Second,
		DialKeepAliveTime:    time.Second * 100,
		DialKeepAliveTimeout: time.Second * 10,
	})

	if err != nil {
		base.RedFatal("init etcd err:%s", err.Error())
	}

	naming, err = betcd.NewNamingWithClient(etcdClient, ServiceApp)
	if err != nil {
		base.RedFatal("init etcd naming err:%s", err.Error())
	}
}

func main() {
	go logging()

	_, err := naming.Register(30, endpoints.Endpoint{
		Addr: "127.0.0.1:8082",
		Metadata: map[string]interface{}{
			"version": "1.0.0",
			"weight":  10,
			"auth":    true,
		},
	})

	if err != nil {
		base.RedFatal("register err:%s", err.Error())
	}

	_, err = naming.Register(30, endpoints.Endpoint{
		Addr: "127.0.0.1:8083",
		Metadata: map[string]interface{}{
			"version": "1.0.0",
			"weight":  100,
			"auth":    true,
		},
	})
	if err != nil {
		base.RedFatal("register err:%s", err.Error())
	}

	var done chan struct{}
	<-done
}

func logging() {
	tick := time.NewTicker(time.Second)

	for range tick.C {
		endpointMap, err := naming.List(context.TODO())
		if err != nil {
			base.Red("list endpoint err:%s", err.Error())
			continue
		}

		base.Green("%+v", endpointMap)
	}
}

```
