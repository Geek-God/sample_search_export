// Package mysqlModel
// @author:WXZ
// @date:2022/12/2
// @note

package mysqlModel

import (
	mysqlInit "sample_search_export/initd/mysqlInitd"
	"time"
)

// 自定义任务导出表
type SampleSearchExport struct {
	ID          int    `gorm:"column:id;type:int(10) unsigned;primary_key;AUTO_INCREMENT"`
	Type        int    `gorm:"column:type;type:tinyint(1) unsigned;default:1;NOT NULL"`       //  1 简单搜索 2 复杂搜索 3 表达式搜索 4 上传
	Condition   string `gorm:"column:condition;type:text"`                                    // 查询条件
	Fields      string `gorm:"column:fields;type:text;NOT NULL"`                              // 导出字段
	Status      int   `gorm:"column:status;type:tinyint(3) unsigned;default:0;NOT NULL"`     // 导出状态 0：未开始 1：进行中 2：已结束 3：失败
	DownUrl     string `gorm:"column:down_url;type:varchar(255)"`                             // 下载地址
	UpdateTime  string `gorm:"column:update_time;type:datetime;NOT NULL"`                     // 更新导出状态时间
	Remark      string `gorm:"column:remark;type:varchar(255)"`                               // 备注
	ExportCount int    `gorm:"column:export_count;type:int(10) unsigned;default:0;NOT NULL"` // 导出数量
}

func (m *SampleSearchExport) TableName() string {
	return "sample_search_export"
}

// ExportList
// @Author WXZ
// @Description: //TODO
// @receiver m *SampleSearchExport
// @return []SampleSearchExport
// @return error
func (m *SampleSearchExport) ExportList() ([]SampleSearchExport, error) {
	db, err := mysqlInit.Client()
	if err != nil {
		return nil, err
	}
	out := []SampleSearchExport{}
	db.Select("*").Where("status=?", 0).Find(&out)
	return out, nil
}

// Update
// @Author WXZ
// @Description: //TODO
// @receiver m *SampleSearchExport
func (m *SampleSearchExport) Update() error {
	db, err := mysqlInit.Client()
	if err != nil {
		return err
	}
	m.UpdateTime = time.Now().Format("2006-01-02 15:04:05")
	return db.Save(m).Error
}
