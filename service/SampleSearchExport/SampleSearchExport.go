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
	"sample_search_export/model/elasticModel"
	"sample_search_export/model/mysqlModel"
	"strings"
	"time"
)

var (
	slice_field []string
	ch          chan map[string]interface{} = make(chan map[string]interface{}, 100)
)

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
		if v.Type == 4 {
			file_type, err := fileType(v.Condition)
			if err != nil {
				return err
			}
			switch file_type {
			case 0:
				readLineFile(&v)
			case 1:
				readFileBlock(&v)
			}
		} else {
			elasticExport(&v)
		}
	}
	return nil
}

// writeFile
// @Author WXZ
// @Description: //TODO
// @param s *mysqlModel.SampleSearchExport
// @return error
func writeFile(s *mysqlModel.SampleSearchExport) error {
	file, err := os.Open("./gch.csv")
	if err != nil {
		return err
	}
	//关闭文件
	defer file.Close()
	//往文件里面写入内容，得到了一个writer对象
	writer := bufio.NewWriter(file)
	line := ""
	writer.WriteString(s.Condition + "\n")
	for {
		v, ok := <-ch
		if ok {
			line = ""
			for _, value := range slice_field {
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
func elasticExport(s *mysqlModel.SampleSearchExport) error {
	search_after := []interface{}{}
	ctx := context.Background()
	sample := elasticModel.Samples{}
	source := elastic.NewFetchSourceContext(true).Include(s.Fields)
	query := elastic.NewBoolQuery()
	query.Must(
		elastic.NewQueryStringQuery(s.Condition),
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
			ch <- data
		}
	}
}

// fileExport
// @Author WXZ
// @Description: //TODO
// @param s *mysqlModel.SampleSearchExport
// @return error
func fileExport(fields string, sha1 []interface{}) error {
	search_after := []interface{}{}
	ctx := context.Background()
	sample := elasticModel.Samples{}
	source := elastic.NewFetchSourceContext(true).Include(fields)
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
			ch <- data
		}
	}
}

// ReadFile
// @Author WXZ
// @Description: //TODO
// @param filePath string
// @param handle func(string)
// @return error
func readLineFile(s *mysqlModel.SampleSearchExport) error {
	f, err := os.Open(s.Condition)
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
					fileExport(s.Fields, sha1)
				}
				return nil
			}
			return err
		}
		line = strings.TrimSpace(line)
		sha1 = append(sha1, line)

		if y := index % 1000; y == 0 {
			fileExport(s.Fields, sha1)
			sha1 = sha1[:0]
		}
	}

}

// readBlock
// @Author WXZ
// @Description: //TODO
// @param filename string
// @return content []byte
func readFileBlock(s *mysqlModel.SampleSearchExport) (content []byte) {
	startTime := time.Now()
	//打开文件
	fileHandler, err := os.Open(s.Condition)
	if err != nil {
		log.Println(err.Error())
		return
	}
	//关闭文件
	defer fileHandler.Close()
	buffer := make([]byte, 41)
	sha1 := make([]interface{}, 0, 1000)
	index := 0
	for {
		n, err := fileHandler.Read(buffer)
		if err != nil && err != io.EOF {
			log.Println(err.Error())
		}
		//读取完成
		if n == 0 {
			if len(sha1) > 0 {
				fileExport(s.Fields, sha1)
			}
			break
		}
		str := string(buffer)
		str = strings.Trim(str, ",")
		sha1 = append(sha1, str)

		if y := index % 1000; y == 0 {
			fileExport(s.Fields, sha1)
			sha1 = sha1[:0]
		}
	}
	fmt.Println("读取的内容长度：", len(content))
	fmt.Println("运行时间：", time.Now().Sub(startTime))
	return
}

// fileType
// @Author WXZ
// @Description: //TODO
// @param path string
// @return int
// @return error
func fileType(path string) (int, error) {
	file, err := os.Open(path)
	defer file.Close()

	if err != nil {
		return -1, err
	}
	tmp := make([]byte, 41)
	_, err = file.Read(tmp)
	if err != nil {
		return -1, err
	}
	str := string(tmp)
	if i := strings.Index(str, ","); i > 0 {
		return 1, nil
	}
	return 0, nil
}
