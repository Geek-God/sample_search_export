// Package mysqlInitd
// @author:WXZ
// @date:2022/12/1
// @note

package mysqlInit

import (
	"fmt"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mysql"
	"github.com/spf13/viper"
	"sample_search_export/conststat"
)

type mysqler struct {
	d *gorm.DB
}

var m *mysqler

// openDB
// @Author WXZ
// @Description: //TODO 创建数据库连接池
// @param username string
// @param password string
// @param host string
// @param charset string
// @param database string
// @param port int
// @return *gorm.DB
// @return error
func openDB(username, password, host, charset, database string, port int) (*gorm.DB, error) {
	connPath := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=%s&parseTime=%t&loc=%s", username, password, host, port, database, charset, true, "Local")
	db, err := gorm.Open("mysql", connPath)
	if err != nil {
		return nil, err
	}

	db.DB().SetMaxOpenConns(conststat.MYSQL_MAX_OPEN_CONNS)
	db.DB().SetMaxIdleConns(conststat.MYSQL_MAX_IDLE_CONNS)
	db.DB().SetConnMaxLifetime(conststat.MYSQL_MAX_LIFE_TIME)

	return db, nil
}

//	Init
//	@Author WXZ
//	@Description:
func New() error {
	//如果有连接则不初始化
	if m != nil && m.d != nil {
		Close()
	}

	db, err := GetDb()
	if err != nil {
		return err
	}

	m = &mysqler{
		db,
	}
	return nil
}

//	GetDb
//	@Author WXZ
//	@Description: 获取连接
//	@return *gorm.DB
func GetDb() (*gorm.DB, error) {
	if m != nil && m.d != nil && m.d.DB().Ping() == nil {
		return m.d, nil
	}
	return openDB(
		viper.GetString("mysql.username"),
		viper.GetString("mysql.password"),
		viper.GetString("mysql.host"),
		viper.GetString("mysql.charset"),
		viper.GetString("mysql.database"),
		viper.GetInt("mysql.port"),
	)
}
func Close() error {
	return m.close()
}

//	Close
//	@Author WXZ
//	@Description: 关闭连接
//	@receiver c *Conn
//	@return error
func (m *mysqler) close() error {
	return m.d.Close()
}
