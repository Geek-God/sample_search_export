// Package elasticModel
// @author:WXZ
// @date:2022/12/1
// @note

package elasticModel

import (
	"context"
	"github.com/olivere/elastic/v6"
	"sample_search_export/initd/elasticInitd"
)

type Interface interface {
	GetIndex() string
	GetType() string
}

type Model struct {
	ID int `json:"id"`
}

//	BachInsert
//	@Author WXZ
//	@Description: //TODO 批量插入
//	@param jsonData string
//	@param esIndex string
//	@param esType string
//	@return error
func BachInsert(ctx context.Context, model Interface, jsonDataMap map[string]string) error {
	es, err := elasticInit.Client()
	if err != nil {
		return err
	}

	bluck := es.Bulk()
	for key, value := range jsonDataMap {
		bluckRequest := elastic.NewBulkIndexRequest().
			Index(model.GetIndex()).
			Type(model.GetType()).
			Id(key).
			Doc(value)
		bluck.Add(bluckRequest)
	}
	_, err = bluck.Do(ctx)
	return err
}

// Update
// @Author WXZ
// @Description: //TODO
// @param ctx context.Context
// @param model Interface
// @param id string
// @param doc interface{}
// @return error
func Update(ctx context.Context, model Interface, id string, doc interface{}) error {
	es, err := elasticInit.Client()
	if err != nil {
		return err
	}

	_, err = es.Update().Index(model.GetIndex()).Type(model.GetType()).Id(id).Doc(doc).Do(ctx)
	return err
}

//	GetQuery
//	@Author WXZ
//	@Description: //TODO Query查询
//	@param esIndex string
//	@param esType string
//	@param query elasticCmd.Query
//	@param offset int
//	@param limit int
//	@return string 数据为Json格式
//	@return error
func GetList(ctx context.Context, model Interface, query elastic.Query, source *elastic.FetchSourceContext, searchAfter []interface{}, limit int, sortAsc bool) (*elastic.SearchResult, error) {
	es, err := elasticInit.Client()
	if err != nil {
		return nil, err
	}

	service := es.Search().Index(model.GetIndex()).Type(model.GetType())
	if len(searchAfter) > 0 {
		service = service.SearchAfter(searchAfter...)
	}

	result, err := service.Query(query).
		From(0).
		Size(limit).
		Pretty(true).
		Sort("_id", sortAsc).
		FetchSourceContext(source).
		Do(ctx)
	return result, err
}

// GetInfoById
// @Author WXZ
// @Description: //TODO
// @param ctx context.Context
// @param model Interface
// @param id string
// @param source *elastic.FetchSourceContext
func GetInfoById(ctx context.Context, model Interface, id string, fields ...string) (*elastic.SearchResult, error) {
	var (
		query    elastic.Query = elastic.NewTermQuery("id", id)
		source   *elastic.FetchSourceContext
	)

	if len(fields) > 0 {
		source = elastic.NewFetchSourceContext(true)
		source.Include(fields...)
	}

	es, err := elasticInit.Client()
	if err != nil {
		return nil, err
	}
	result, err := es.Search().Index(model.GetIndex()).Type(model.GetType()).
		Query(query).
		Size(1).
		FetchSourceContext(source).
		Do(ctx)

	return result, err
}

// GetListAggs
// @Author WXZ
// @Description: //TODO
// @param model Interface
// @param query elastic.Query
// @param aggs elastic.Aggregations
// @return *elastic.SearchResult
// @return error
func GetListAggs(ctx context.Context, model Interface, query elastic.Query, aggs elastic.Aggregation) (*elastic.SearchResult, error) {
	es, err := elasticInit.Client()
	if err != nil {
		return nil, err
	}

	service := es.Search().Index(model.GetIndex()).Type(model.GetType())
	result, err := service.Query(query).
		From(0).
		Size(0).
		Pretty(true).
		Aggregation("aggs_data", aggs).
		Do(ctx)
	return result, err
}

