package betcd

import (
	"go.etcd.io/etcd/client/v3"
)

// Client etcd client
type Client interface {
	// Connection 获取ectd client
	Connection() (client *clientv3.Client)
	// Close ---
	Close() (err error)
}

func NewClient(conf *clientv3.Config) (c Client, err error) {
	var conn *clientv3.Client
	conn, err = clientv3.New(*conf)

	if err != nil {
		return nil, err
	}

	c = &client{
		connection: conn,
	}

	return c, err
}

type client struct {
	connection *clientv3.Client
}

func (c *client) Connection() (client *clientv3.Client) {
	return c.connection
}

func (c *client) Close() (err error) {
	return c.connection.Close()
}
