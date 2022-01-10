package betcd

import (
	"github.com/grpc-boot/base"
)

type Deserialize func(data []byte) (value interface{}, err error)

func JsonMapDeserialize(data []byte) (value interface{}, err error) {
	var val map[string]interface{}
	err = base.JsonDecode(data, &val)
	return val, err
}

func YamlMapDeserialize(data []byte) (value interface{}, err error) {
	var val map[string]interface{}
	err = base.YamlDecode(data, &val)
	return val, err
}

func StringDeserialize(data []byte) (value interface{}, err error) {
	return base.Bytes2String(data), nil
}
