package redis

import (
	"context"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/storacha/indexing-service/pkg/redis"
)

// BlackholeRedis is a client that does not retain any data.
type BlackholeRedis struct{}

var _ redis.Client = (*BlackholeRedis)(nil)

func NewBlackholeRedis() *BlackholeRedis {
	return &BlackholeRedis{}
}

func (bh *BlackholeRedis) Expire(ctx context.Context, key string, expiration time.Duration) *goredis.BoolCmd {
	return goredis.NewBoolCmd(ctx, nil)
}

func (bh *BlackholeRedis) Get(ctx context.Context, key string) *goredis.StringCmd {
	cmd := goredis.NewStringCmd(ctx, nil)
	cmd.SetErr(goredis.Nil)
	return cmd
}

func (m *BlackholeRedis) Persist(ctx context.Context, key string) *goredis.BoolCmd {
	return goredis.NewBoolCmd(ctx, nil)
}

func (m *BlackholeRedis) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *goredis.StatusCmd {
	return goredis.NewStatusCmd(ctx, nil)
}

func (m *BlackholeRedis) SAdd(ctx context.Context, key string, values ...interface{}) *goredis.IntCmd {
	cmd := goredis.NewIntCmd(ctx, nil)
	cmd.SetVal(int64(len(values)))
	return cmd
}

func (m *BlackholeRedis) SMembers(ctx context.Context, key string) *goredis.StringSliceCmd {
	cmd := goredis.NewStringSliceCmd(ctx, nil)
	cmd.SetErr(goredis.Nil)
	return cmd
}
