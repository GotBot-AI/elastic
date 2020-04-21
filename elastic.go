package elastic

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
)

// HOW TO USE //////////////////////////////////////////////////////
// //Create client connection
// client := elastic.Create(elastic.ClientOptions{URI: "http://localhost:9200"})

// //Index one or more documents - or update using same id
// j := []string{`{"_id":"123", "name":"cat"}, {"_id":"124", "name":"dog"}`}
// client.IndexMany("animals", j)

// //Search for documents
// name := "gotbot"
// query := fmt.Sprintf(`{
// 		"query":{
// 			"match":{
// 				"name":"%v"
// 			}
// 		}
// 	}`, name)
// 	r := client.Search("customers", query)

//ClientOptions - connection settings and options for elasticsearch
type ClientOptions struct {
	URI string
}

//Client - Elastic client object
type Client struct {
	ES *elasticsearch.Client
}

var wg sync.WaitGroup

//Create - Create Elastic client object
func Create(co ClientOptions) Client {

	if co.URI == "" {
		// fmt.Println("No url")
		es, err := elasticsearch.NewDefaultClient()
		if err != nil {
			log.Fatal(err)
		}
		log.Println(elasticsearch.Version)
		log.Println(es.Info())
		return Client{ES: es}
	}
	cfg := elasticsearch.Config{
		Addresses: []string{
			co.URI,
		},
	}
	es, err := elasticsearch.NewClient(cfg)
	if err != nil {
		log.Fatal(err)
	}
	return Client{ES: es}
}

func (clnt *Client) MapIndexs(m []map[string]string) {
	for i, doc := range m {
		wg.Add(1)
		go func(i int, doc map[string]string) {
			defer wg.Done()
			index := doc["index"]
			body := doc["map"]
			is := []string{index}
			check := esapi.IndicesExistsRequest{
				Index: is,
			}
			checkres, err := check.Do(context.Background(), clnt.ES)
			if err != nil {
				fmt.Println(err)
				return
			}
			fmt.Println(checkres.StatusCode)
			if checkres.StatusCode == 404 {
				clnt.CreateIndex(index, body)
			} else if checkres.StatusCode == 200 {
				//Update index
				fmt.Println("Index already exists")
			}
		}(i, doc)
	}
	wg.Wait()
}
func (clnt *Client) CreateIndex(index string, body string) {
	req := esapi.IndicesCreateRequest{
		Index:      index,
		Body:       strings.NewReader(body),
		Pretty:     false,
		Human:      false,
		ErrorTrace: false,
		FilterPath: nil,
		Header:     nil,
	}
	// Perform the request with the client.
	res, err := req.Do(context.Background(), clnt.ES)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("Created index", res.StatusCode)
}

func (clnt *Client) IndexOne(index string, doc string, indexKey string) {
	var d map[string]interface{}
	json.Unmarshal([]byte(doc), &d)
	_id := d[indexKey].(string)
	if indexKey == "_id" {
		delete(d, "_id")
	}
	j, _ := json.Marshal(d)
	req := esapi.IndexRequest{
		Index:      index,
		DocumentID: _id,
		Body:       strings.NewReader(string(j)),
		Refresh:    "true",
	}
	// Perform the request with the client.
	res, err := req.Do(context.Background(), clnt.ES)
	if err != nil {
		log.Println(res)
		//log.Fatalf("Error getting response: %s", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		log.Println(res)
		//log.Printf("[%s] Error indexing document", res.Status())
	} else {
		var r map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
			log.Println(res)
		} else {
			log.Printf("[%s] %s; version=%d", res.Status(), r["result"], int(r["_version"].(float64)))
		}
	}
}

//IndexMany - index one or many documents
func (clnt *Client) IndexMany(index string, jsonArr []string, indexKey string) {
	for i, doc := range jsonArr {
		wg.Add(1)
		go func(i int, doc string) {
			defer wg.Done()
			var d map[string]interface{}
			json.Unmarshal([]byte(doc), &d)
			_id := d[indexKey].(string)
			if indexKey == "_id" {
				delete(d, "_id")
			}
			j, _ := json.Marshal(d)
			req := esapi.IndexRequest{
				Index:      index,
				DocumentID: _id,
				Body:       strings.NewReader(string(j)),
				Refresh:    "true",
			}
			// Perform the request with the client.
			res, err := req.Do(context.Background(), clnt.ES)
			if err != nil {
				log.Fatalf("Error getting response: %s", err)
			}
			defer res.Body.Close()

			if res.IsError() {
				log.Println(res)
			} else {

				// Deserialize the response into a map.
				var r map[string]interface{}
				if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
					log.Printf("Error parsing the response body: %s", err)
				} else {
					// Print the response status and indexed document version.
					log.Printf("[%s] %s; version=%d", res.Status(), r["result"], int(r["_version"].(float64)))
				}
			}
		}(i, doc)
	}
	wg.Wait()
}

func (clnt *Client) Search(index string, query string) []map[string]interface{} {

	bytes := bytes.NewBufferString(query)
	res, err := clnt.ES.Search(
		clnt.ES.Search.WithContext(context.Background()),
		clnt.ES.Search.WithIndex(index),
		clnt.ES.Search.WithBody(bytes),
		clnt.ES.Search.WithTrackTotalHits(true),
		clnt.ES.Search.WithPretty(),
	)

	if err != nil {
		log.Fatalf("Error getting response: %s", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		var e map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
			log.Fatalf("Error parsing the response body: %s", err)
		} else {
			// Print the response status and error information.
			log.Fatalf("[%s] %s: %s",
				res.Status(),
				e["error"].(map[string]interface{})["type"],
				e["error"].(map[string]interface{})["reason"],
			)
		}
	}

	var r map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		log.Fatalf("Error parsing the response body: %s", err)
	}
	// Print the response status, number of results, and request duration.
	log.Printf(
		"[%s] %d hits; took: %dms",
		res.Status(),
		int(r["hits"].(map[string]interface{})["total"].(map[string]interface{})["value"].(float64)),
		int(r["took"].(float64)),
	)
	hits := r["hits"].(map[string]interface{})["hits"].([]interface{})
	var results []map[string]interface{}
	for _, item := range hits {
		i, ok := item.(map[string]interface{})
		if !ok {
			log.Fatal("Error parsing map")
		}
		s := i["_source"].(map[string]interface{})
		s["_id"] = i["_id"]
		results = append(results, s)
	}
	return results
}
