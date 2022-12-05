// Package sample_search_export
// @author:WXZ
// @date:2022/11/30
// @note

package main

import (
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"log"
	"os"
	"sample_search_export/config"
	elasticInit "sample_search_export/initd/elasticInitd"
	"sample_search_export/initd/logrusInit"
	"sample_search_export/initd/minioInitd"
	mysqlInit "sample_search_export/initd/mysqlInitd"
	"sample_search_export/service/SampleSearchExport"
	"sample_search_export/utils"
	"time"
)

func init() {
	log.SetFlags(log.LstdFlags)

	config.New()
	logrusInit.New()
	err := mysqlInit.New()
	if err != nil {
		log.Fatal(err)
	}
	err = elasticInit.New()
	if err != nil {
		log.Fatal(err)
	}
	err = minioInitd.New()
	if err != nil {
		log.Fatal(err)
	}
	err = timeLoc()
	if err != nil {
		log.Fatal(err)
	}
	err = inspect()
	if err != nil {
		log.Fatal(err)
	}
}
func main() {
	for {
		err := SampleSearchExport.Export()
		if err != nil {
			logrus.Error(err.Error())
		}
		time.Sleep(300 * time.Second)
	}
}

// makeDir
// @Author WXZ
// @Description: //TODO 运行前检查
// @return error
func inspect() error {
	if ok := utils.FileExists(viper.GetString("export.path")); !ok {
		return os.MkdirAll(viper.GetString("export.path"), 0766)
	}
	return nil
}

// timeLoc
// @Author WXZ
// @Description: //TODO 设置时区
func timeLoc() error {
	loc, err := time.LoadLocation(viper.GetString("timezone"))
	if err != nil {
		return err
	}
	time.Local = loc
	return nil
}
