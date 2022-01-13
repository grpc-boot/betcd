package betcd

import (
	"context"
	"testing"
	"time"

	"github.com/grpc-boot/base"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/naming/endpoints"
)

var (
	client   *clientv3.Client
	name     Naming
	caseList = []endpoints.Endpoint{
		{Addr: "127.0.0.1:8080", Metadata: "qa1"},
		{Addr: "127.0.0.1:8081", Metadata: "qa2"},
		{Addr: "127.0.0.1:8082", Metadata: "qa3"},
		{Addr: "127.0.0.1:8083", Metadata: "qa4"},
	}
)

func init() {
	var err error
	client, err = clientv3.New(clientv3.Config{
		Endpoints:   []string{"10.16.49.131:2379"},
		DialTimeout: time.Second * 3,
	})

	if err != nil {
		base.RedFatal("init client err:%s", err.Error())
	}

	name, err = NewNamingWithClient(client, "browser/service/app")
	if err != nil {
		base.RedFatal("init naming err:%s", err.Error())
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

func TestNaming_List(t *testing.T) {
	tick := time.NewTicker(time.Second)

	for range tick.C {
		eps, err := name.List(context.TODO())
		if err != nil {
			t.Fatalf("list err:%s", err.Error())
		}

		data, _ := base.JsonEncode(eps)
		t.Logf("list info:%s", data)
	}
}
