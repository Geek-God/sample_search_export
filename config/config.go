// Package config
// @author:WXZ
// @date:2022/12/1
// @note

package config

import (
	"fmt"
	"github.com/spf13/viper"
)

func init() {
	viper.SetConfigName("config")    // 配置文件名，不需要后缀名
	viper.SetConfigType("yaml")      // 配置文件格式
	viper.AddConfigPath("./conf/") // 查找配置文件的路径
	viper.AddConfigPath(".")         // 查找配置文件的路径
	err := viper.ReadInConfig()      // 查找并读取配置文件
	if err != nil {                  // 处理错误
		panic(fmt.Errorf("Fatal error config file: %w \n", err))
	}
}
func New() {

}
