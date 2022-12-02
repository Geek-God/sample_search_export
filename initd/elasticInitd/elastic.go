// Package elasticInitd
// @author:WXZ
// @date:2022/12/1
// @note
package elasticInit

import (
	"context"
	"errors"
	"fmt"

	"github.com/olivere/elastic/v6"
	"github.com/spf13/viper"
)

type elasticer struct {
	c *elastic.Client
}

var e *elasticer

//	openEs
//	@Author WXZ
//	@Description:
//	@return *elasticModel.Client
func openEs(host string) (*elastic.Client, error) {
	client, err := elastic.NewClient(elastic.SetSniff(false), elastic.SetURL(host))
	if err != nil {
		return nil, err
	}
	_, code, err := client.Ping(host).Do(context.Background())

	if err != nil {
		return nil, errors.New(fmt.Sprintf("elasticInitd initd pint error:%v,code:%d\n", err, code))
	}

	return client, nil
}

//	GetEs
//	@Author WXZ
//	@Description:
//	@return *elasticModel.Client
func New() (*elastic.Client, error) {
	if e != nil && e.c != nil {
		return e.c, nil
	}

	client, err := openEs(
		viper.GetString("elasticInitd.host"),
	)

	if err != nil {
		return nil, err
	}

	e.c = client
	return client, nil
}
