package betcd

import (
	"github.com/grpc-boot/base"
)

const (
	String = "string"
	Json   = "json"
	Yaml   = "Yaml"
)

// KeyOption Key配置
type KeyOption struct {
	Key         string `json:"key" yaml:"key"`
	Type        string `json:"type" yaml:"type"`
	Deserialize Deserialize
}

// Deserialize 反序列化函数
type Deserialize func(data []byte) (value interface{}, err error)

// Deserializer 反序列化工具
type Deserializer interface {
	// Deserialize 反序列化
	Deserialize(key string, val []byte) (value interface{}, err error)
}

// NewDeserializer 实例化反序列化工具对象
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

// JsonMapDeserialize Json反序列化为map[string]interface{}
func JsonMapDeserialize(data []byte) (value interface{}, err error) {
	var val map[string]interface{}
	err = base.JsonDecode(data, &val)
	return val, err
}

// YamlMapDeserialize Yaml反序列化为map[string]interface{}
func YamlMapDeserialize(data []byte) (value interface{}, err error) {
	var val map[string]interface{}
	err = base.YamlDecode(data, &val)
	return val, err
}

// StringDeserialize 字符串
func StringDeserialize(data []byte) (value interface{}, err error) {
	return base.Bytes2String(data), nil
}
