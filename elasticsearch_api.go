package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch"
	"github.com/elastic/go-elasticsearch/esapi"
	"os"
	"strconv"
)

var c *elasticsearch.Client

// 连接搜索引擎
func init() {
	var err error
	config := elasticsearch.Config{}
	config.Addresses = []string{"http://192.168.99.102:9200"}
	c, err = elasticsearch.NewClient(config)
	checkError(err)
}

func checkError(err error) {
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

// 添加索引
func createIndex(es_index string, ec_type string) {
	body := map[string]interface{}{
		"mappings": map[string]interface{}{
			ec_type: map[string]interface{}{
				"properties": map[string]interface{}{
					"str": map[string]interface{}{
						"type": "keyword", // 表示这个字段不分词
					},
				},
			},
		},
	}
	jsonBody, _ := json.Marshal(body)
	req := esapi.IndicesCreateRequest{
		Index: es_index,
		Body:  bytes.NewReader(jsonBody),
	}
	res, err := req.Do(context.Background(), c)
	checkError(err)
	defer res.Body.Close()
	fmt.Println(res.String())
}

// 删除索引
func deleteIndex(es_index string) {
	req := esapi.IndicesDeleteRequest{
		Index: []string{es_index},
	}
	res, err := req.Do(context.Background(), c)
	checkError(err)
	defer res.Body.Close()
	fmt.Println(res.String())
}

// 插入单条数据
func insertSingle(num, v int, str string, es_index string, es_type string) {
	body := map[string]interface{}{
		"num": num,
		"v":   v,
		"str": str,
	}
	jsonBody, _ := json.Marshal(body)

	req := esapi.IndexRequest{
		Index:        es_index,
		Body:         bytes.NewReader(jsonBody),
		DocumentType: es_type,
	}
	res, err := req.Do(context.Background(), c)
	checkError(err)
	defer res.Body.Close()
	fmt.Println(res.String())
}

// 批量插入
func insertBatch(es_index string, es_type string) {
	var bodyBuf bytes.Buffer
	for i := 2; i < 10; i++ {
		createLine := map[string]interface{}{
			"create": map[string]interface{}{
				"_index": es_index,
				"_id":    "test_" + strconv.Itoa(i),
				"_type":  es_type,
			},
		}
		jsonStr, _ := json.Marshal(createLine)
		bodyBuf.Write(jsonStr)
		bodyBuf.WriteByte('\n')

		body := map[string]interface{}{
			"num": i % 3,
			"v":   i,
			"str": "test" + strconv.Itoa(i),
		}
		jsonStr, _ = json.Marshal(body)
		bodyBuf.Write(jsonStr)
		bodyBuf.WriteByte('\n')
	}

	req := esapi.BulkRequest{
		Body: &bodyBuf,
	}
	res, err := req.Do(context.Background(), c)
	checkError(err)
	defer res.Body.Close()
	fmt.Println(res.String())
}

// 通过sql查询
func selectBySql() {
	query := map[string]interface{}{
		"query": "select count(*) as cnt, max(v) as value, num from test_index where num > 0 group by num",
	}
	jsonBody, _ := json.Marshal(query)
	req := esapi.XPackSQLQueryRequest{
		Body: bytes.NewReader(jsonBody),
	}
	res, err := req.Do(context.Background(), c)
	checkError(err)
	defer res.Body.Close()
	fmt.Println(res.String())

}

// 通过Search Api查询
func selectBySearchAll(es_index string, ec_type []string) {
	//组织自己的查询语句
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"match_all": map[string]interface{}{},
		},
	}

	jsonBody, _ := json.Marshal(query)

	req := esapi.SearchRequest{
		Index:        []string{es_index},
		Body:         bytes.NewReader(jsonBody),
		DocumentType: ec_type,
	}
	res, err := req.Do(context.Background(), c)
	checkError(err)
	defer res.Body.Close()
	fmt.Println(res.String())
}

// 根据单一条件查询，此处以num为例
func selectBySearchSome(es_index string, ec_type []string) {
	//组织自己的查询语句
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"match": map[string]interface{}{
				"num": "2",
			},
		},
	}

	jsonBody, _ := json.Marshal(query)

	req := esapi.SearchRequest{
		Index:        []string{es_index},
		Body:         bytes.NewReader(jsonBody),
		DocumentType: ec_type,
	}
	res, err := req.Do(context.Background(), c)
	checkError(err)
	defer res.Body.Close()
	fmt.Println(res.String())
}

// 根据id更新
func updateSingle(es_index string, es_type string) {
	body := map[string]interface{}{
		"doc": map[string]interface{}{
			"v": 100,
		},
	}
	jsonBody, _ := json.Marshal(body)
	req := esapi.UpdateRequest{
		Index:        es_index,
		DocumentID:   "test_3",
		Body:         bytes.NewReader(jsonBody),
		DocumentType: es_type,
		Pretty:       true,
	}

	res, err := req.Do(context.Background(), c)
	checkError(err)
	defer res.Body.Close()
	fmt.Println(res.String())
}

// 根据条件更新
func updateByQuery(es_index string, es_type []string) {
	body := map[string]interface{}{
		"script": map[string]interface{}{
			"lang": "painless",
			"source": `
                ctx._source.v = params.value;
            `,
			"params": map[string]interface{}{
				"value": 1000,
			},
		},
		"query": map[string]interface{}{
			"match_all": map[string]interface{}{},
		},
	}
	jsonBody, _ := json.Marshal(body)
	req := esapi.UpdateByQueryRequest{
		Index:        []string{es_index},
		Body:         bytes.NewReader(jsonBody),
		DocumentType: es_type,
	}
	res, err := req.Do(context.Background(), c)
	checkError(err)
	defer res.Body.Close()
	fmt.Println(res.String())
}

//根据id删除
func deleteSingle(es_index string, es_type string) {
	req := esapi.DeleteRequest{
		Index:        es_index,
		DocumentID:   "test_3",
		DocumentType: es_type,
	}

	res, err := req.Do(context.Background(), c)
	checkError(err)
	defer res.Body.Close()
	fmt.Println(res.String())
}

// 根据条件删除
func deleteByQuery(es_index string, es_type []string) {
	body := map[string]interface{}{
		"query": map[string]interface{}{
			"match_all": map[string]interface{}{},
		},
	}
	jsonBody, _ := json.Marshal(body)
	req := esapi.DeleteByQueryRequest{
		Index:        []string{es_index},
		Body:         bytes.NewReader(jsonBody),
		DocumentType: es_type,
	}
	res, err := req.Do(context.Background(), c)
	checkError(err)
	defer res.Body.Close()
	fmt.Println(res.String())
}

func main() {
	//createIndex("school", "school_type")
	//deleteIndex("school")
	//insertSingle(10, 6, "zhangsan", "school", "school_type")
	//insertBatch("school", "school_type")
	//selectBySearchAll("school", []string{"school_type"})
	//selectBySearchSome("school", []string{"school_type"})
	//updateSingle("school", "school_type")
	//updateByQuery("school", []string{"school_type"})
	//deleteSingle("school", "school_type")
	//deleteByQuery("school", []string{"school_type"})
}
