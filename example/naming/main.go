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
