package betcd

import (
	"context"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/naming/endpoints"
	"google.golang.org/grpc"
)

type Naming interface {
	// List 列表
	List(ctx context.Context) (endpoints endpoints.Key2EndpointMap, err error)
	// Add 添加
	Add(endpoint endpoints.Endpoint, opts ...clientv3.OpOption) (err error)
	// AddContext with context添加
	AddContext(ctx context.Context, endpoint endpoints.Endpoint, opts ...clientv3.OpOption) (err error)
	// Register 注册
	Register(keepAliveSecond int64, endpoint endpoints.Endpoint, opts ...clientv3.OpOption) (ch <-chan *clientv3.LeaseKeepAliveResponse, err error)
	// Del 删除
	Del(endpoint endpoints.Endpoint, opts ...clientv3.OpOption) (err error)
	// DelContext with context删除
	DelContext(ctx context.Context, endpoint endpoints.Endpoint, opts ...clientv3.OpOption) (err error)
	// ChangeEndpoint 更换
	ChangeEndpoint(ctx context.Context, new endpoints.Endpoint) (err error)
	// DialGrpc ---
	DialGrpc(opts ...grpc.DialOption) (*grpc.ClientConn, error)
}

type naming struct {
	client   *clientv3.Client
	service  string
	manager  endpoints.Manager
	grpcAddr string
}

func NewNaming(v3Conf *clientv3.Config, service string) (n Naming, err error) {
	client, err := clientv3.New(*v3Conf)

	if err != nil {
		return nil, err
	}

	return NewNamingWithClient(client, service)
}

func NewNamingWithClient(client *clientv3.Client, service string) (n Naming, err error) {
	manager, err := endpoints.NewManager(client, service)
	if err != nil {
		return nil, err
	}

	n = &naming{
		client:   client,
		service:  service,
		manager:  manager,
		grpcAddr: "etcd:///" + service,
	}

	return
}

func (n *naming) etcdKey(key string) string {
	return n.service + "/" + key
}

func (n *naming) List(ctx context.Context) (endpoints endpoints.Key2EndpointMap, err error) {
	return n.manager.List(ctx)
}

func (n *naming) Add(endpoint endpoints.Endpoint, opts ...clientv3.OpOption) (err error) {
	return n.AddContext(context.TODO(), endpoint, opts...)
}

func (n *naming) AddContext(ctx context.Context, endpoint endpoints.Endpoint, opts ...clientv3.OpOption) (err error) {
	return n.manager.AddEndpoint(ctx, n.etcdKey(endpoint.Addr), endpoint, opts...)
}

func (n *naming) Register(keepAliveSecond int64, endpoint endpoints.Endpoint, opts ...clientv3.OpOption) (ch <-chan *clientv3.LeaseKeepAliveResponse, err error) {

	lease := clientv3.NewLease(n.client)
	lresp, err := lease.Grant(n.client.Ctx(), keepAliveSecond)
	if err != nil {
		return nil, err
	}

	if len(opts) < 1 {
		opts = make([]clientv3.OpOption, 0, 1)
	}
	opts = append(opts, clientv3.WithLease(lresp.ID))

	err = n.Add(endpoint, opts...)
	if err != nil {
		return nil, err
	}
	return n.client.KeepAlive(n.client.Ctx(), lresp.ID)
}

func (n *naming) Del(endpoint endpoints.Endpoint, opts ...clientv3.OpOption) (err error) {
	return n.DelContext(context.TODO(), endpoint, opts...)
}

func (n *naming) DelContext(ctx context.Context, endpoint endpoints.Endpoint, opts ...clientv3.OpOption) (err error) {
	return n.manager.DeleteEndpoint(ctx, n.etcdKey(endpoint.Addr), opts...)
}

func (n *naming) ChangeEndpoint(ctx context.Context, endpoint endpoints.Endpoint) (err error) {
	return n.manager.Update(ctx, []*endpoints.UpdateWithOpts{
		endpoints.NewDeleteUpdateOpts(n.etcdKey(endpoint.Addr)),
		endpoints.NewAddUpdateOpts(n.etcdKey(endpoint.Addr), endpoint),
	})
}

func (n *naming) DialGrpc(opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	return grpc.Dial(n.grpcAddr, opts...)
}
