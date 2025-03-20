package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/tedjadhi/example-go-redis-round-robin/roundrobin"

	"github.com/redis/go-redis/v9"
)

func main() {
	// Initialize Redis client
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   0,
	})

	// Create WaitGroup for goroutines
	var wg sync.WaitGroup

	// Create context
	ctx := context.Background()

	// Create round-robin instance
	rr := roundrobin.New(client)

	// Add phone numbers
	phoneNumbers := []string{
		"+6281234567890",
		"+6281234567891",
		"+6281234567892",
		"+6281234567893",
	}

	for _, number := range phoneNumbers {
		if err := rr.AddPhoneNumber(ctx, number); err != nil {
			log.Printf("Failed to add phone number %s: %v\n", number, err)
			continue
		}
		log.Printf("Added phone number: %s\n", number)
	}

	// Simulate multiple message sending requests
	for i := 0; i < 100; i++ {
		go func(i int) {
			// Simulate lock phone number
			time.Sleep(2000 * time.Millisecond)
			rr.SetPhoneNumberLockLimited(ctx, "+6281234567890", 50*time.Minute)
			rr.SetPhoneNumberLockLimited(ctx, "+6281234567891", 50*time.Minute)
		}(i)
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			number, err := rr.GetNextPhoneNumber(ctx)
			if err != nil {
				log.Printf("Failed to get next phone number: %v\n", err)
				return
			}
			log.Printf("Sending message using phone number: %s\n", number)

			// Simulate message sending delay
			time.Sleep(time.Second)
		}(i)
	}

	// Wait for all goroutines to finish
	wg.Wait()

	// Summary Counting
	for _, phone := range phoneNumbers {
		c, _ := client.Get(context.Background(), fmt.Sprintf("message_gateway:counter:%s", phone)).Int64()
		fmt.Printf("%s - %d\n", phone, c)
	}

	// flush all
	client.FlushAll(context.Background())
	// close client
	client.Close()
}
