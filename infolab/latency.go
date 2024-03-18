package infolab

import (
	"fmt"
	"strconv"
	"time"

	"github.com/go-redis/redis"
	"github.com/spf13/viper"
)

const redisKeyForPendingBatchStartTime = "pbst"
const redisKeyForPendingTransactionStartTime = "ptst"

var redisClient *redis.Client

func OpenRedisClient() {
	var redisAddress = fmt.Sprintf(
		"%s:%d",
		viper.GetString("peer.redis.host"),
		viper.GetInt("peer.redis.port"),
	)
	fmt.Printf("Initialize: Redis 접속: %s\n", redisAddress)
	if redisAddress == ":0" {
		redisClient = redis.NewClient(&redis.Options{
			Addr:     "master.jjo.kr:16379",
			Password: "",
			DB:       0,
		})
	} else {
		redisClient = redis.NewClient(&redis.Options{
			Addr:     redisAddress,
			Password: viper.GetString("peer.redis.password"),
			DB:       viper.GetInt("peer.redis.database"),
		})
	}
}
func AddPendingBatchStartTime(num uint64, t *time.Time) {
	if redisClient == nil {
		OpenRedisClient()
	}
	var key = fmt.Sprintf("%s-%d", redisKeyForPendingBatchStartTime, num)

	if err := redisClient.Set(key, t.Format(time.RFC3339Nano), 0).Err(); err != nil {
		panic(err)
	}
}
func ResolvePendingBatchStartTime(num uint64) time.Duration {
	if redisClient == nil {
		OpenRedisClient()
	}
	var key = fmt.Sprintf("%s-%d", redisKeyForPendingBatchStartTime, num)

	if chunk, err := redisClient.Get(key).Result(); err == nil {
		t, err := time.Parse(time.RFC3339Nano, chunk)
		if err != nil {
			panic(err)
		}
		// go redisClient.Del(key)
		return time.Since(t)
	}
	fmt.Printf("레디스에 없는 블록: %d\n", num)
	return 0
}
func ResolvePendingTransactionStartTime(txId string) time.Duration {
	if redisClient == nil {
		OpenRedisClient()
	}
	var key = fmt.Sprintf("%s-%s", redisKeyForPendingTransactionStartTime, txId)

	if chunk, err := redisClient.Get(key).Result(); err == nil {
		c, err := strconv.ParseInt(chunk, 10, 64)
		if err != nil {
			panic(err)
		}
		go redisClient.Del(key)
		return time.Since(time.Unix(0, c*int64(time.Millisecond)))
	}
	// fmt.Println("레디스에 없는 트랜잭션: " + txId)
	return 0
}
