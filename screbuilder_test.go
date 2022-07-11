package ytrebuilder

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"testing"

	"github.com/elastic/go-elasticsearch/v7"
	"github.com/tidwall/gjson"
)

var esCli *elasticsearch.Client

func TestMain(m *testing.M) {
	config := elasticsearch.Config{
		Addresses: []string{"http://127.0.0.1:9200"},
		Username:  "elastic",
		Password:  "7uji9olp-",
	}
	var err error
	esCli, err = elasticsearch.NewClient(config)
	if err != nil {
		log.Fatal(err)
	}
	os.Exit(m.Run())
}

func TestRead(t *testing.T) {
	query := `{"query": {"match_all" : {}},"size": 100}`
	var b strings.Builder
	b.WriteString(query)
	read := strings.NewReader(b.String())
	res, err := esCli.Search(
		esCli.Search.WithContext(context.Background()),
		esCli.Search.WithIndex("verifyerr"),
		esCli.Search.WithBody(read),
		esCli.Search.WithTrackTotalHits(true),
		esCli.Search.WithPretty(),
	)
	if err != nil {
		t.Error(err)
	}
	if res.IsError() {
		t.Error(err)
	}
	fmt.Println(res)
	var resb bytes.Buffer
	resb.ReadFrom(res.Body)
	hit := gjson.Get(resb.String(), "hits.total.value")
	fmt.Printf("hit: %d\n", hit.Int())
	vals := gjson.Get(resb.String(), "hits.hits")
	requests := make([]*NodeRebuildRequest, 0, hit.Int())
	err = json.NewDecoder(strings.NewReader(vals.String())).Decode(&requests)
	if err != nil {
		t.Error(err)
	}
	fmt.Printf("hits: %v\n", vals)
}
