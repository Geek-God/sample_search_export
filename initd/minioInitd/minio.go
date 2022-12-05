// Package minioInitd
// @author:WXZ
// @date:2022/2/23
// @note

package minioInitd

import (
	"github.com/minio/minio-go"
	"github.com/spf13/viper"
)

type minioer struct {
	c *minio.Client
}

var m *minioer

// new
// @Author WXZ
// @Description: //TODO
// @param host string
// @param accessKey string
// @param secretKey string
// @param secure bool
// @return *minio.Client
// @return error
func new(host, accessKey, secretKey string, secure bool) (*minio.Client, error) {
	return minio.New(host, accessKey, secretKey, secure)
}

// New
// @Author WXZ
// @Description: //TODO
// @return error
func New() error {
	conn, err := new(viper.GetString("minio.host"),
		viper.GetString("minio.access_key"),
		viper.GetString("minio.secret_key"),
		false,
	)
	if err != nil {
		return err
	}
	m = &minioer{
		conn,
	}
	return nil
}

// Client
// @Author WXZ
// @Description: //TODO
// @return *minio.Client
func Client() (*minio.Client, error) {
	if m == nil {
		if err := New(); err != nil {
			return nil, err
		}
	}
	return m.c, nil
}
