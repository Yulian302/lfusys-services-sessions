package main

import "context"

type service struct {
	Store
}

func NewService(s Store) *service {
	return &service{s}
}

func (s *service) Create(context.Context) error {
	return nil
}
