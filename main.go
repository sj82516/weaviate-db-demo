package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/weaviate/weaviate-go-client/v4/weaviate"
	"github.com/weaviate/weaviate-go-client/v4/weaviate/data/replication"
	"github.com/weaviate/weaviate-go-client/v4/weaviate/filters"
	"github.com/weaviate/weaviate-go-client/v4/weaviate/graphql"
	"github.com/weaviate/weaviate/entities/models"
)

const (
	className = "Book"
)

var (
	client *weaviate.Client
	cfg    = weaviate.Config{
		Host:   "localhost:8080",
		Scheme: "http",
	}
)

type Additional struct {
	ID       string  `json:"id"`
	Distance float64 `json:"distance"`
}

type Book struct {
	Title      string     `json:"title"`
	Type       string     `json:"type"`
	Additional Additional `json:"_additional"`
}

func main() {
	var err error
	client, err = weaviate.NewClient(cfg)
	if err != nil {
		panic(err)
	}

	deleteClass()

	// batch import data
	batchImport()

	// search for books
	search([]string{"Hello"})
}

func search(concepts []string) {
	nearText := client.GraphQL().NearTextArgBuilder().
		WithConcepts(concepts).
		// move close to the word "Yellow"
		WithMoveTo(&graphql.MoveParameters{
			Force: 0.5,
			Concepts: []string{
				"Yellow",
			},
		})

	where := filters.Where().
		WithPath([]string{"type"}).
		WithOperator(filters.Equal).
		WithValueText("program")

	fields := []graphql.Field{
		{Name: "title"},
		{Name: "_additional", Fields: []graphql.Field{
			{Name: "id"},
			{Name: "distance"},
		}},
	}

	result, err := client.GraphQL().Get().
		WithClassName(className).
		WithNearText(nearText).
		WithWhere(where).
		WithFields(fields...).
		Do(context.Background())

	if result.Errors != nil {
		for _, err := range result.Errors {
			fmt.Println(err.Message)
		}
		return
	}

	r := result.Data["Get"].(map[string]interface{})[className].([]interface{})

	jsonbody, err := json.Marshal(r)
	if err != nil {
		// do error check
		fmt.Println(err)
		return
	}

	books := []Book{}
	if err := json.Unmarshal(jsonbody, &books); err != nil {
		// do error check
		fmt.Println(err)
		return
	}

	for _, book := range books {
		fmt.Println(book)
	}
}

func batchImport() {
	client.Schema().ClassCreator().WithClass(&models.Class{
		Class:       className,
		Description: "all books I have",
		Vectorizer:  "text2vec-transformers",
		ModuleConfig: map[string]interface{}{
			"text2vec-transformers": map[string]interface{}{},
		},
		Properties: []*models.Property{
			{
				Name:     "title",
				DataType: []string{"text"},
			},
		},
	})

	objects := []*models.Object{
		{
			Class: className,
			Properties: map[string]interface{}{
				"title": "Hello World Blue",
				"type":  "program",
			},
		},
		{
			Class: className,
			Properties: map[string]interface{}{
				"title": "Hello World Red",
				"type":  "program",
			},
		}, {
			Class: className,
			Properties: map[string]interface{}{
				"title": "Hello World Yellow",
				"type":  "science",
			},
		},
	}

	client.Batch().ObjectsBatcher().
		WithObjects(objects...).
		WithConsistencyLevel(replication.ConsistencyLevel.ALL).
		Do(context.Background())
}

func deleteClass() {
	client.Schema().ClassDeleter().
		WithClassName(className).
		Do(context.Background())
}
