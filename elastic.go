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
			fmt.Println(checkres.StatusCode)
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
				log.Fatalf("Error getting response: %s", err)
			}
			defer res.Body.Close()

			if res.IsError() {
				log.Printf("[%s] Error indexing document %s ID=%d", res.Status(), res.String(), i+1)
			} else {
				// Deserialize the response into a map.
				var r map[string]interface{}
				if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
					log.Printf("Error parsing the response body: %s", err)
				} else {
					// Print the response status and indexed document version.
					//log.Printf("[%s] %s; version=%d", res.Status(), r["result"], int(r["_version"].(float64)))
					fmt.Println(res)
				}
			}
		}(i, doc)
	}
	wg.Wait()
}

//IndexMany - index one or many documents
func (clnt *Client) IndexMany(index string, jsonArr []string, indexKey string) {
	for i, doc := range jsonArr {
		wg.Add(1)
		go func(i int, doc string) {
			defer wg.Done()
			noJsonString := strings.Replace(doc, `\`, "", -1)
			fixId := strings.Replace(noJsonString, `_id`, "id", -1)
			fmt.Println(fixId)
			var d map[string]interface{}
			json.Unmarshal([]byte(fixId), &d)
			id := d[indexKey].(string)
			req := esapi.IndexRequest{
				Index:      index,
				DocumentID: id,
				Body:       strings.NewReader(fixId),
				Refresh:    "true",
			}

			// Perform the request with the client.
			res, err := req.Do(context.Background(), clnt.ES)
			if err != nil {
				log.Fatalf("Error getting response: %s", err)
			}
			defer res.Body.Close()

			if res.IsError() {
				log.Printf("[%s] Error indexing document ID=%d", res.Status(), i+1)
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

func (clnt *Client) Search(index string, query string) map[string]interface{} {

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
	// Print the ID and document source for each hit.

	// for _, hit := range r["hits"].(map[string]interface{})["hits"].([]interface{}) {
	// 	fmt.Println(hit)
	// 	log.Printf(" * ID=%s, %s", hit.(map[string]interface{})["_id"], hit.(map[string]interface{})["_source"])
	// }
	return r
}
