package betcd

import (
	"context"
	"encoding/xml"
	"log"
	"testing"
	"time"

	"github.com/grpc-boot/base"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/naming/endpoints"
)

var (
	client   *clientv3.Client
	conf     Config
	name     Naming
	caseList = []endpoints.Endpoint{
		{Addr: "127.0.0.1:8080", Metadata: "qa1"},
		{Addr: "127.0.0.1:8081", Metadata: "qa2"},
		{Addr: "127.0.0.1:8082", Metadata: "qa3"},
		{Addr: "127.0.0.1:8083", Metadata: "qa4"},
	}

	keyOptions = []KeyOption{
		{Key: "browser/conf/app/version", Type: String},
		{Key: "browser/conf/app/limit", Type: Json},
		{Key: "browser/conf/app/redis", Type: Yaml},
		{Key: "browser/conf/app/mysql", Deserialize: func(data []byte) (value interface{}, err error) {
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
	client, err = clientv3.New(clientv3.Config{
		Endpoints:   []string{"127.0.0.1:2379"},
		DialTimeout: time.Second * 3,
	})

	if err != nil {
		base.RedFatal("init client err:%s", err.Error())
	}

	name, err = NewNamingWithClient(client, "browser/service/app")
	if err != nil {
		base.RedFatal("init naming err:%s", err.Error())
	}

	conf, err = NewConfigWithClient(client, []string{"browser/conf/app", "browser/conf/web"}, keyOptions)

	if err != nil {
		base.Red("init kv err:%s", err.Error())
	}

	loging()
}

func loging() {
	tick := time.NewTicker(time.Second)

	for range tick.C {
		eps, err := name.List(context.TODO())
		if err != nil {
			log.Fatalf("list err:%s", err.Error())
		}

		data, _ := base.JsonEncode(eps)
		log.Printf("list info:%s", data)

		for _, opt := range keyOptions {
			val, exists := conf.Get(opt.Key)
			log.Printf("conf get[%s]-%t-%+v", opt.Key, exists, val)
		}
	}
}

func TestNaming_Add(t *testing.T) {
	for _, ep := range caseList {
		if err := name.Add(ep); err != nil {
			t.Fatalf("add endpoint err:%s", err.Error())
		}
	}

	lease := clientv3.NewLease(client)
	rsp, err := lease.Grant(client.Ctx(), 10)
	if err != nil {
		t.Fatalf("create lease err:%s", err.Error())
	}

	err = name.Add(endpoints.Endpoint{
		Addr:     "127.0.0.1:9000",
		Metadata: "php-fpm",
	}, clientv3.WithLease(rsp.ID))

	if err != nil {
		t.Fatalf("add endpoint with lease err:%s", err.Error())
	}
}

func TestNaming_Register(t *testing.T) {
	_, err := name.Register(10, endpoints.Endpoint{
		Addr:     "127.0.0.1:3306",
		Metadata: "mysql",
	})

	if err != nil {
		t.Fatalf("register service err:%s", err.Error())
	}
}

func TestNaming_DialGrpc(t *testing.T) {
	conn, err := name.DialGrpc()
	if err != nil {
		t.Fatalf("dial grpc error:%s", err.Error())
	}

	t.Logf("dial conn:%+v", conn)
}

func TestConfig_Put(t *testing.T) {
	_, err := conf.Put(keyOptions[0].Key, time.Now().String(), time.Second)
	if err != nil {
		t.Fatalf("put err:%s", err.Error())
	}

	_, err = conf.Put(keyOptions[1].Key, `{"zone":60, "count": 50, "list":[{"h":"c"},{"h":"n"}]}`, time.Second)
	if err != nil {
		t.Fatalf("put err:%s", err.Error())
	}

	redisConf := map[string]interface{}{
		"host": "127.0.0.1",
		"port": 6379,
		"auth": "asdfadf",
		"db":   4,
	}

	val, _ := base.YamlEncode(redisConf)

	_, err = conf.Put(keyOptions[2].Key, base.Bytes2String(val), time.Second)
	if err != nil {
		t.Fatalf("put err:%s", err.Error())
	}

	mysqlConf := MysqlOption{
		Host:     "127.0.0.1",
		Port:     3306,
		DbName:   "betcd",
		UserName: "sadfesadf",
		Password: "betcd",
	}

	val, _ = xml.Marshal(mysqlConf)
	_, err = conf.Put(keyOptions[3].Key, base.Bytes2String(val), time.Second)
	if err != nil {
		t.Fatalf("put err:%s", err.Error())
	}

	loging()
}
