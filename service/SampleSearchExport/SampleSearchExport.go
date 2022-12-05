// Package mysqlModel
// @author:WXZ
// @date:2022/12/2
// @note

package SampleSearchExport

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"github.com/olivere/elastic/v6"
	"io"
	"log"
	"os"
	"sample_search_export/conststat"
	"sample_search_export/model/elasticModel"
	"sample_search_export/model/mysqlModel"
	"sample_search_export/utils"
	"strings"
	"sync"
)

// task
// @Author WXZ
// @Description: //TODO
type task struct {
	sliceField []string
	ch         chan map[string]interface{}
	exportInfo *mysqlModel.SampleSearchExport
	sy         sync.WaitGroup
}

// Export
// @Author WXZ
// @Description: //TODO
// @return error
func Export() error {
	m := &mysqlModel.SampleSearchExport{}
	list, err := m.ExportList()
	if err != nil {
		return err
	}
	for _, v := range list {
		v.Status = conststat.STATUS_START
		err = v.Update()
		if err != nil {
			log.Fatal(err)
		}
		t := &task{
			sliceField: strings.Split(v.Fields, ","),
			ch:         make(chan map[string]interface{}, 1000),
			exportInfo: &v,
		}
		t.sy.Add(2)
		go t.writeFile()
		if v.Type == conststat.SEARCH_UPLOAD {
			err = t.fileExport()
		} else {
			err = t.elasticExport()
		}
		t.sy.Wait()

		if err != nil {
			t.exportInfo.Status = conststat.STATUS_FAIL
			t.exportInfo.Remark = err.Error()

		} else {
			t.exportInfo.Status = conststat.STATUS_END
		}
		if err = t.exportInfo.Update(); err != nil {
			return err
		}
	}
	return nil
}

// writeFile
// @Author WXZ
// @Description: //TODO 结果写入文件
// @param s *mysqlModel.SampleSearchExport
// @return error
func (t *task) writeFile() error {
	defer t.sy.Done()

	file_name := fmt.Sprintf("./%d.csv", t.exportInfo.ID)
	if utils.FileExists(file_name) {
		if err := os.Remove(file_name); err != nil {
			return err
		}
	}
	file, err := os.OpenFile(file_name, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0766)
	if err != nil {
		return err
	}
	//关闭文件
	defer file.Close()
	//往文件里面写入内容，得到了一个writer对象
	writer := bufio.NewWriter(file)
	line := ""
	writer.WriteString(t.exportInfo.Fields + "\n")
	for {
		v, ok := <-t.ch
		if !ok {
			break
		}

		line = ""
		for _, value := range t.sliceField {
			switch (v[value]).(type) {
			case []string:
				slice_str := (v[value]).([]string)
				str := strings.Join(slice_str, ",")
				line += fmt.Sprintf("\"%s\",", str)
			default:
				fmt.Println(fmt.Sprintf("\"%v\",", v[value]))
				line += fmt.Sprintf("\"%v\",", v[value])
			}
		}
		if line != "" {
			writer.WriteString(strings.Trim(line, ",") + "\n")
			writer.Flush()
		}

	}
	//将缓存中内容的写入文件
	return writer.Flush()
}

// elasticExport
// @Author WXZ
// @Description: //TODO es查询导出
// @param s mysqlModel.SampleSearchExport
// @return error
func (t *task) elasticExport() error {
	defer t.sy.Done()
	defer close(t.ch)

	search_after := []interface{}{}
	ctx := context.Background()
	size := 1000

	sample := elasticModel.Samples{}
	source := elastic.NewFetchSourceContext(true).Include(t.sliceField...)
	bool_query := elastic.NewBoolQuery()
	var query []elastic.Query

	switch t.exportInfo.Type {
	//简单搜索
	case conststat.SEARCH_SIMPLE:
		simples := strings.Split(t.exportInfo.Condition, ",")
		//判读是否为搜索sha1
		ok, err := utils.SampleSha1Metch(simples[0])
		if err != nil {
			return err
		}
		terms_field := "sha1"
		if !ok {
			terms_field = "id"
		}
		query = append(query, elastic.NewTermsQuery(terms_field, simples))
	default:
		query = append(query, elastic.NewQueryStringQuery(t.exportInfo.Condition))
	}

	bool_query.Must(query...)
	for {
		result, err := elasticModel.GetList(ctx, sample, bool_query, source, search_after, 1000, true)
		if err != nil {
			return err
		}
		if result == nil || result.Hits == nil || result.Hits.Hits == nil || len(result.Hits.Hits) <= 0 {
			return nil
		}
		search_after = result.Hits.Hits[len(result.Hits.Hits)-1].Sort

		for _, value := range result.Hits.Hits {
			if value.Source == nil {
				continue
			}
			data := make(map[string]interface{}, 1)
			err := json.Unmarshal(*value.Source, &data)
			if err != nil {
				continue
			}
			t.ch <- data
		}
		if len(result.Hits.Hits) < size {
			break
		}
	}
	return nil
}

// fileExport
// @Author WXZ
// @Description: //TODO 上传sha1文件导出
// @param s *mysqlModel.SampleSearchExport
// @return error
func (t *task) sha1Export(sha1 []interface{}) error {
	search_after := []interface{}{}
	ctx := context.Background()
	sample := elasticModel.Samples{}
	source := elastic.NewFetchSourceContext(true).Include(t.sliceField...)
	query := elastic.NewBoolQuery()
	query.Must(
		elastic.NewTermsQuery("sha1", sha1...),
	)

	result, err := elasticModel.GetList(ctx, sample, query, source, search_after, len(sha1), true)
	if err != nil {
		return err
	}
	if result == nil || result.Hits == nil || result.Hits.Hits == nil || len(result.Hits.Hits) <= 0 {
		return nil
	}

	for _, value := range result.Hits.Hits {
		if value.Source == nil {
			continue
		}
		data := make(map[string]interface{}, 1)
		err := json.Unmarshal(*value.Source, &data)
		if err != nil {
			continue
		}
		t.ch <- data
	}

	return nil
}

// ReadFile
// @Author WXZ
// @Description: //TODO 按照行读取
// @param filePath string
// @param handle func(string)
// @return error
func (t *task) readLineFile() error {
	f, err := os.Open(t.exportInfo.Condition)
	if err != nil {
		return err
	}
	defer f.Close()

	buf := bufio.NewReader(f)
	size := 1000
	sha1 := make([]interface{}, 0, size)
	for {
		line, err := buf.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				if len(sha1) > 0 {
					t.sha1Export(sha1)
				}
				return nil
			}
			return err
		}

		line = strings.TrimSpace(line)
		sha1 = append(sha1, line)

		if len(sha1) > size {
			t.sha1Export(sha1)
			sha1 = sha1[:0]
		}
	}
	return nil
}

// readBlock
// @Author WXZ
// @Description: //TODO 按照指定长度读取
// @param filename string
// @return content []byte
func (t *task) readFileBlock() error {
	//打开文件
	file, err := os.Open(t.exportInfo.Condition)
	if err != nil {
		return err
	}
	//关闭文件
	defer file.Close()
	buffer := make([]byte, 41)
	size := 1000
	sha1 := make([]interface{}, 0, size)

	for {
		n, err := file.Read(buffer)
		if err != nil {
			if err == io.EOF {
				if len(sha1) > 0 {
					t.sha1Export(sha1)
				}
				return nil
			}
			return err
		}

		str := string(buffer[:n])
		str = strings.Trim(str, ",")
		sha1 = append(sha1, str)

		if len(sha1) >= size {
			t.sha1Export(sha1)
			sha1 = sha1[:0]
		}
	}
	return nil
}

// fileType
// @Author WXZ
// @Description: //TODO 文件上传导出
// @param path string
// @return int
// @return error
func (t *task) fileExport() error {
	defer t.sy.Done()
	defer close(t.ch)

	file, err := os.Open(t.exportInfo.Condition)
	defer file.Close()

	if err != nil {
		return err
	}
	tmp := make([]byte, 41)
	_, err = file.Read(tmp)
	if err != nil {
		return err
	}
	str := string(tmp)
	if i := strings.Index(str, ","); i > 0 {
		return t.readFileBlock()
	} else {
		return t.readLineFile()
	}
}
