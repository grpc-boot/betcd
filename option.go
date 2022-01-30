package betcd

// ConfigOption 配置选项
type ConfigOption struct {
	PrefixList    []string    `json:"prefixList" yaml:"prefixList"`
	KeyOptionList []KeyOption `json:"keyOptionList" yaml:"keyOptionList"`
	ChannelSize   uint16      `json:"channelSize" yaml:"channelSize"`
}
