// Package _const
// @author:WXZ
// @date:2022/11/30
// @note

package conststat

import "time"

var (
	ROOT_PATH string = "./"
	//数据库连接池最大连接数
	MYSQL_MAX_OPEN_CONNS int = 20
	//最大空闲数
	MYSQL_MAX_IDLE_CONNS int = 5
	//可用连接最长时间
	MYSQL_MAX_LIFE_TIME time.Duration = time.Second * 600
)
