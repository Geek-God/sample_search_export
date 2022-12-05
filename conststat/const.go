// Package _const
// @author:WXZ
// @date:2022/11/30
// @note

package conststat

import "time"

const (
	ROOT_PATH string = "./"
	//数据库连接池最大连接数
	MYSQL_MAX_OPEN_CONNS int = 5
	//最大空闲数
	MYSQL_MAX_IDLE_CONNS int = 2
	//可用连接最长时间
	MYSQL_MAX_LIFE_TIME time.Duration = time.Second * 600

	//导出任务状态
	// 0：未开始 1：进行中 2：已结束 3：失败
	STATUS_NOT_START = 0
	STATUS_START     = 1
	STATUS_END       = 2
	STATUS_FAIL      = 3

	// 搜索类型
	// 1 简单搜索 2 复杂搜索 3 表达式搜索 4 上传
	SEARCH_SIMPLE     = 1
	SEARCH_COMPLEX    = 2
	SEARCH_EXPRESSION = 3
	SEARCH_UPLOAD     = 4
)
