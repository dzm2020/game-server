package profile

import (
	fileutil "game-server/pkg/profile"

	"github.com/spf13/viper"
)

var (
	vip  *viper.Viper
	base = DefaultNodeBaseConfig()
)

func Init(path string) {
	v, err := fileutil.ViperLoadConfigWithInclude(path)
	if err != nil {
		panic(err)
	}
	vip = v
	if err = Get("base", &base); err != nil {
		panic(err)
	}
}

func Get(key string, dest any) error {
	if vip == nil {
		return viper.New().UnmarshalKey(key, dest)
	}
	return vip.UnmarshalKey(key, dest)
}

func GetBase() *NodeBaseConfig {
	return base
}
