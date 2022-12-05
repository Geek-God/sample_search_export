// Package sample_search_export
// @author:WXZ
// @date:2022/11/30
// @note

package sample_search_export

import (
	"sample_search_export/config"
	"sample_search_export/initd/logrusInit"
	"sample_search_export/initd/minioInitd"
	mysqlInit "sample_search_export/initd/mysqlInitd"
)

func init() {
	config.New()
	logrusInit.New()
	mysqlInit.New()
	minioInitd.New()
}
func main() {

}
