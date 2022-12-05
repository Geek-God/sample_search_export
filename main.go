// Package sample_search_export
// @author:WXZ
// @date:2022/11/30
// @note

package main

import (
	"github.com/spf13/viper"
	"log"
	"sample_search_export/config"
	elasticInit "sample_search_export/initd/elasticInitd"
	"sample_search_export/initd/logrusInit"
	mysqlInit "sample_search_export/initd/mysqlInitd"
	"sample_search_export/service/SampleSearchExport"
	"time"
)

func init() {
	config.New()
	loc, err := time.LoadLocation(viper.GetString("timezone"))
	if err != nil {
		log.Fatal(err)
	}
	time.Local = loc

	logrusInit.New()
	mysqlInit.New()
	elasticInit.New()
	//minioInitd.New()
}
func main() {
	err := SampleSearchExport.Export()
	log.Fatal(err)
}
