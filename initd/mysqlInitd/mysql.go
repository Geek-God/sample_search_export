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
	db *gorm.DB
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

// Client
// @Author WXZ
// @Description: //TODO
// @return *gorm.DB
// @return error
func Client() (*gorm.DB, error) {
	if m != nil && m.db != nil && m.db.DB().Ping() == nil {
		return m.db, nil
	}
	if err := New(); err != nil {
		return nil, err
	}
	return m.db, nil
}

// New
// @Author WXZ
// @Description: //TODO
// @return error
func New() error {
	conn, err := openDB(
		viper.GetString("mysql.username"),
		viper.GetString("mysql.password"),
		viper.GetString("mysql.host"),
		viper.GetString("mysql.charset"),
		viper.GetString("mysql.database"),
		viper.GetInt("mysql.port"),
	)
	if err != nil {
		return err
	}
	m = &mysqler{
		conn,
	}
	return nil
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
	return m.db.Close()
}
