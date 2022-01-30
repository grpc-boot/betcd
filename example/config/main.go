package main

import (
	"encoding/xml"
	"time"

	"github.com/grpc-boot/base"
	"github.com/grpc-boot/base/core/shardmap"
	"github.com/grpc-boot/betcd"
	clientv3 "go.etcd.io/etcd/client/v3"
)

var (
	etcdClient *clientv3.Client
	config     betcd.Config
	changeChan <-chan shardmap.ChangeEvent
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
		Endpoints:            []string{"10.16.49.131:2379"},
		DialTimeout:          time.Second,
		DialKeepAliveTime:    time.Second * 100,
		DialKeepAliveTimeout: time.Second * 10,
	})

	if err != nil {
		base.RedFatal("init etcd err:%s", err.Error())
	}

	confOption := betcd.ConfigOption{
		PrefixList:    []string{"browser/conf/app"},
		KeyOptionList: keyOptions,
		ChannelSize:   1024, //事件channel大小，如果不需要监听缓存事件变化，大小设置为0即可
	}

	config, changeChan, err = betcd.NewConfigWithClient(etcdClient, confOption)
	if err != nil {
		base.RedFatal("init etcd config err:%s", err.Error())
	}
}

func main() {
	go logging()

	go consume()

	go func() {
		for {
			produce()
			time.Sleep(time.Millisecond * 500)
		}
	}()

	defer config.Close()

	var done chan struct{}
	<-done
}

func produce() {
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
}

func consume() {
	for {
		event, ok := <-changeChan
		if !ok {
			break
		}

		switch event.Type {
		case shardmap.Create:
			base.Fuchsia("create key:%+v value:%+v", event.Key, event.Value)
		case shardmap.Update:
			base.Fuchsia("update key:%+v value:%+v", event.Key, event.Value)
		case shardmap.Delete:
			base.Fuchsia("delete key:%+v value:%+v", event.Key, event.Value)
		}
	}
}

func logging() {
	tick := time.NewTicker(time.Second * 5)
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
