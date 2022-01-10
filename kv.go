package betcd

const (
	String = "string"
	Json   = "json"
	Yaml   = "Yaml"
)

type KeyOption struct {
	Key         string `json:"key" yaml:"key"`
	Type        string `json:"type" yaml:"type"`
	Deserialize Deserialize
}
