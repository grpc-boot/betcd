package betcd

import (
	"context"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/naming/endpoints"
	etcdresolver "go.etcd.io/etcd/client/v3/naming/resolver"
	"google.golang.org/grpc"
	"google.golang.org/grpc/resolver"
)

type Naming interface {
	// Add 注册服务
	Add(endpoint endpoints.Endpoint, opts ...clientv3.OpOption) (err error)
	// AddContext with context注册服务
	AddContext(ctx context.Context, endpoint endpoints.Endpoint, opts ...clientv3.OpOption) (err error)
	// AddWithLease with lease注册服务
	AddWithLease(ctx context.Context, leaseId clientv3.LeaseID, endpoint endpoints.Endpoint, opts ...clientv3.OpOption) (err error)
	// Del 撤销服务
	Del(endpoint endpoints.Endpoint, opts ...clientv3.OpOption) (err error)
	// DelContext with context撤销服务
	DelContext(ctx context.Context, endpoint endpoints.Endpoint, opts ...clientv3.OpOption) (err error)
	// ChangeEndpoint 更换Endpoint
	ChangeEndpoint(ctx context.Context, new endpoints.Endpoint) (err error)
}

type naming struct {
	client       *clientv3.Client
	service      string
	manager      endpoints.Manager
	grpcAddr     string
	etcdResolver resolver.Builder
}

func NewNaming(v3Conf *clientv3.Config, service string) (n Naming, err error) {
	client, err := clientv3.New(*v3Conf)

	if err != nil {
		return nil, err
	}

	manager, err := endpoints.NewManager(client, service)
	if err != nil {
		return nil, err
	}

	rl, _ := etcdresolver.NewBuilder(client)

	n = &naming{
		client:       client,
		service:      service,
		manager:      manager,
		grpcAddr:     "etcd:///" + service,
		etcdResolver: rl,
	}

	return
}

func (n *naming) etcdKey(key string) string {
	return n.service + "/" + key
}

func (n *naming) Add(endpoint endpoints.Endpoint, opts ...clientv3.OpOption) (err error) {
	return n.AddContext(context.TODO(), endpoint, opts...)
}

func (n *naming) AddContext(ctx context.Context, endpoint endpoints.Endpoint, opts ...clientv3.OpOption) (err error) {
	return n.manager.AddEndpoint(ctx, n.etcdKey(endpoint.Addr), endpoint, opts...)
}

func (n *naming) AddWithLease(ctx context.Context, leaseId clientv3.LeaseID, endpoint endpoints.Endpoint, opts ...clientv3.OpOption) (err error) {
	if len(opts) < 1 {
		opts = make([]clientv3.OpOption, 0, 1)
	}
	opts = append(opts, clientv3.WithLease(leaseId))
	return n.AddContext(ctx, endpoint, opts...)
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

func (n *naming) DialGrpc() (*grpc.ClientConn, error) {
	return grpc.Dial(n.grpcAddr, grpc.WithResolvers(n.etcdResolver))
}
