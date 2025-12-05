package main

import "context"

type DynamoDbStore struct {
	// add here dynamodb
}

func NewStore() *DynamoDbStore {
	return &DynamoDbStore{}
}

func (s *DynamoDbStore) Create(context.Context) error {
	return nil
}
