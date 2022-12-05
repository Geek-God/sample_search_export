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
	"os"
	"sample_search_export/model/elasticModel"
	"sample_search_export/model/mysqlModel"
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
		v.Status = 1
		v.Update()
		t := &task{
			sliceField: strings.Split(v.Condition, ","),
			ch:         make(chan map[string]interface{}, 1000),
			exportInfo: &v,
		}
		t.sy.Add(1)
		go t.writeFile()
		if v.Type == 4 {
			err = t.fileExport()
		} else {
			err = t.elasticExport()
		}
		t.sy.Wait()

		if err != nil {
			t.exportInfo.Status = 3
			t.exportInfo.Remark = err.Error()

		} else {
			t.exportInfo.Status = 2
		}
		t.exportInfo.Update()
	}
	return nil
}

// writeFile
// @Author WXZ
// @Description: //TODO
// @param s *mysqlModel.SampleSearchExport
// @return error
func (t *task) writeFile() error {
	defer t.sy.Done()

	file_name := fmt.Sprintf("./%d.csv", t.exportInfo.ID)
	file, err := os.Open(file_name)
	if err != nil {
		return err
	}
	//关闭文件
	defer file.Close()
	//往文件里面写入内容，得到了一个writer对象
	writer := bufio.NewWriter(file)
	line := ""
	writer.WriteString(t.exportInfo.Condition + "\n")
	for {
		v, ok := <-t.ch
		if ok {
			line = ""
			for _, value := range t.sliceField {
				switch v[value].(type) {
				case []string:
					slice_str := (v[value]).([]string)
					str := strings.Join(slice_str, ",")
					line += fmt.Sprintf("\"%s\",", str)
				}
				line += fmt.Sprintf("\"%v\",", v[value])
			}
			if line != "" {
				writer.WriteString(strings.Trim(line, ",") + "\n")
			}
		}
	}
	//将缓存中内容的写入文件
	return writer.Flush()
}

// elasticExport
// @Author WXZ
// @Description: //TODO
// @param s mysqlModel.SampleSearchExport
// @return error
func (t *task) elasticExport() error {
	defer t.sy.Done()
	defer close(t.ch)

	search_after := []interface{}{}
	ctx := context.Background()
	sample := elasticModel.Samples{}
	source := elastic.NewFetchSourceContext(true).Include(t.exportInfo.Fields)
	query := elastic.NewBoolQuery()
	query.Must(
		elastic.NewQueryStringQuery(t.exportInfo.Condition),
	)

	for {
		result, err := elasticModel.GetList(ctx, sample, query, source, search_after, 5000, true)
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
	}
}

// fileExport
// @Author WXZ
// @Description: //TODO
// @param s *mysqlModel.SampleSearchExport
// @return error
func (t *task) sha1Export(sha1 []interface{}) error {
	search_after := []interface{}{}
	ctx := context.Background()
	sample := elasticModel.Samples{}
	source := elastic.NewFetchSourceContext(true).Include(t.exportInfo.Condition)
	query := elastic.NewBoolQuery()
	query.Must(
		elastic.NewTermsQuery("sha1", sha1...),
	)

	for {
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
	}
}

// ReadFile
// @Author WXZ
// @Description: //TODO
// @param filePath string
// @param handle func(string)
// @return error
func (t *task) readLineFile() error {
	f, err := os.Open(t.exportInfo.Condition)
	defer f.Close()
	if err != nil {
		return err
	}
	buf := bufio.NewReader(f)
	sha1 := make([]interface{}, 0, 1000)
	index := 0
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

		if y := index % 1000; y == 0 {
			t.sha1Export(sha1)
			sha1 = sha1[:0]
		}
	}

}

// readBlock
// @Author WXZ
// @Description: //TODO
// @param filename string
// @return content []byte
func (t *task) readFileBlock() error {
	//打开文件
	fileHandler, err := os.Open(t.exportInfo.Condition)
	if err != nil {
		return err
	}
	//关闭文件
	defer fileHandler.Close()
	buffer := make([]byte, 41)
	sha1 := make([]interface{}, 0, 1000)
	index := 0
	for {
		n, err := fileHandler.Read(buffer)
		if err != nil && err != io.EOF {
			return err
		}
		//读取完成
		if n == 0 {
			if len(sha1) > 0 {
				t.sha1Export(sha1)
			}
			break
		}
		str := string(buffer)
		str = strings.Trim(str, ",")
		sha1 = append(sha1, str)

		if y := index % 1000; y == 0 {
			t.sha1Export(sha1)
			sha1 = sha1[:0]
		}
	}
	return nil
}

// fileType
// @Author WXZ
// @Description: //TODO
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
