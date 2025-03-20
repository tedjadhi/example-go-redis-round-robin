package roundrobin

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	defaultPhoneNumbersKey    = "message_gateway:phone_numbers"       // Sorted set storing phone numbers with their scores
	defaultLockKey            = "message_gateway:lock"                // Lock key for ensuring atomic operations
	lastUsedIndexKey          = "message_gateway:last_used_index"     // Key to store the last used index
	counterKeyPrefix          = "message_gateway:counter:"            // Prefix for phone number counter keys
	phoneLockKeyPrefix        = "message_gateway:phone_lock:"         // Prefix for phone number lock keys
	phoneLockLimitedKeyPrefix = "message_gateway:phone_lock_limited:" // Prefix for phone number lock keys
)

// RoundRobin handles the round-robin selection of phone numbers
type RoundRobin struct {
	client          *redis.Client
	phoneNumbersKey string
	lockKey         string
}

// New creates a new RoundRobin instance
func New(client *redis.Client) *RoundRobin {
	return &RoundRobin{
		client:          client,
		phoneNumbersKey: defaultPhoneNumbersKey,
		lockKey:         defaultLockKey,
	}
}

// AddPhoneNumber adds a new phone number to the pool
func (r *RoundRobin) AddPhoneNumber(ctx context.Context, number string) error {
	// Check if number already exists
	score, err := r.client.ZScore(ctx, r.phoneNumbersKey, number).Result()
	if err != redis.Nil && err != nil {
		return err
	}
	if score != 0 {
		return errors.New("phone number already exists")
	}

	// Get the highest score from the sorted set
	scores, err := r.client.ZRevRangeWithScores(ctx, r.phoneNumbersKey, 0, 0).Result()
	if err != nil && err != redis.Nil {
		return err
	}

	// Calculate next score (highest + 1 or 0 if empty)
	nextScore := float64(0)
	if len(scores) > 0 {
		nextScore = scores[0].Score + 1
	}

	// Add new phone number with unique score
	return r.client.ZAdd(ctx, r.phoneNumbersKey, redis.Z{
		Score:  nextScore,
		Member: number,
	}).Err()
}

// SetPhoneNumberLockLimited sets a lock with TTL for a specific phone number
func (r *RoundRobin) SetPhoneNumberLockLimited(ctx context.Context, number string, ttl time.Duration) error {
	// Check if the phone number exists
	_, err := r.client.ZScore(ctx, r.phoneNumbersKey, number).Result()
	if err == redis.Nil {
		return errors.New("phone number does not exist")
	}
	if err != nil {
		return err
	}

	// Set lock with TTL
	lockLimitedKey := phoneLockLimitedKeyPrefix + number
	return r.client.SetNX(ctx, lockLimitedKey, "1", ttl).Err()
}

// GetNextPhoneNumber returns the next available phone number using a consistent round-robin approach
func (r *RoundRobin) GetNextPhoneNumber(ctx context.Context) (string, error) {
	// Get total number of phone numbers
	size, err := r.client.ZCard(ctx, r.phoneNumbersKey).Result()
	if err != nil {
		return "", err
	}
	if size == 0 {
		return "", errors.New("no phone numbers available")
	}

	// Try to acquire lock with retries for atomic operations
	maxRetries := 100
	lockDuration := 10 * time.Second
	timeDelay := 100 * time.Millisecond

	for i := 0; i < maxRetries; i++ {
		locked, err := r.client.SetNX(ctx, r.lockKey, "1", lockDuration).Result()
		if err != nil {
			return "", err
		}
		if locked {
			defer r.client.Del(ctx, r.lockKey)

			// Get the last used score
			lastScore, err := r.client.Get(ctx, lastUsedIndexKey).Float64()
			if err == redis.Nil {
				lastScore = -1
			} else if err != nil {
				return "", err
			}

			// Get all phone numbers with scores greater than last used score
			scores, err := r.client.ZRangeByScoreWithScores(ctx, r.phoneNumbersKey, &redis.ZRangeBy{
				Min: fmt.Sprintf("%f", lastScore),
				Max: "+inf",
			}).Result()
			if err != nil {
				return "", err
			}

			// If no numbers found after lastScore, wrap around to the beginning
			if len(scores) <= 1 {
				scores, err = r.client.ZRangeByScoreWithScores(ctx, r.phoneNumbersKey, &redis.ZRangeBy{
					Min: "-inf",
					Max: "+inf",
				}).Result()
				if err != nil {
					return "", err
				}
			}

			// Find the next available phone number
			var selectedPhone string
			var selectedScore float64
			for _, z := range scores {
				phone := z.Member.(string)
				score := z.Score

				// Skip the current number if it's the last used one
				if score <= lastScore {
					continue
				}

				// Check if the phone is locked
				lockLimitedKey := phoneLockLimitedKeyPrefix + phone
				locked, err := r.client.Exists(ctx, lockLimitedKey).Result()
				if err != nil {
					return "", err
				}
				if locked == 0 {
					selectedPhone = phone
					selectedScore = score
					break
				}
			}

			// If no available phone found, try from the beginning
			if selectedPhone == "" {
				for _, z := range scores {
					phone := z.Member.(string)
					score := z.Score

					lockLimitedKey := phoneLockLimitedKeyPrefix + phone
					locked, err := r.client.Exists(ctx, lockLimitedKey).Result()
					if err != nil {
						return "", err
					}
					if locked == 0 {
						selectedPhone = phone
						selectedScore = score
						break
					}
				}
			}

			// If still no available phone found
			if selectedPhone == "" {
				return "", errors.New("no available phone numbers (all are locked)")
			}

			// Update the last used score
			err = r.client.Set(ctx, lastUsedIndexKey, selectedScore, 0).Err()
			if err != nil {
				return "", err
			}

			// Increment counter for the selected phone number
			counterKey := counterKeyPrefix + selectedPhone
			_, err = r.client.Incr(ctx, counterKey).Result()
			if err != nil {
				return "", err
			}

			return selectedPhone, nil
		}
		if i < maxRetries-1 {
			time.Sleep(timeDelay)
			continue
		}
	}

	return "", errors.New("failed to acquire lock after maximum retries")
}
