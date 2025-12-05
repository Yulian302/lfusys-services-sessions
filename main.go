package main

import "context"

func main() {
	store := NewStore()
	svc := NewService(store)

	svc.Create(context.Background())
}
