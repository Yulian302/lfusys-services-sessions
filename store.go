package main

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

type store struct {
	Client    *dynamodb.Client
	TableName string
}

func NewStore(client *dynamodb.Client, tableName string) *store {
	return &store{
		Client:    client,
		TableName: tableName,
	}
}

func (s *store) Create(context.Context) error {
	return nil
}
