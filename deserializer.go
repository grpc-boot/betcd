package betcd

import (
	"github.com/grpc-boot/base"
)

type Deserialize func(data []byte) (value interface{}, err error)

type Deserializer interface {
	Deserialize(key string, val []byte) (value interface{}, err error)
}

func NewDeserializer(options map[string]KeyOption) Deserializer {
	return &deserializer{keyOptions: options}
}

type deserializer struct {
	keyOptions map[string]KeyOption
}

func (d *deserializer) Deserialize(key string, val []byte) (value interface{}, err error) {
	if d.keyOptions == nil {
		return val, err
	}

	if option, exists := d.keyOptions[key]; exists {
		//优先使用自定义解析
		if option.Deserialize != nil {
			return option.Deserialize(val)
		}

		switch option.Type {
		case Json:
			return JsonMapDeserialize(val)
		case Yaml:
			return YamlMapDeserialize(val)
		case String:
			return StringDeserialize(val)
		}
	}

	return val, err
}

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
