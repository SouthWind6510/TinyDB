package db

import (
	"SouthWind6510/TinyDB/pkg/constants"
	"context"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"testing"

	"github.com/go-redis/redis/v8"
)

func Test_Str(t *testing.T) {
	_ = os.Setenv(constants.DebugEnv, "1")
	tinyDB := openDB(0)
	defer tinyDB.Close()

	if err := tinyDB.Set([]byte("key1"), []byte("value1")); err != nil {
		t.Error("Set failed")
	}

	if res, _ := tinyDB.Get([]byte("key1")); string(res) != "value1" {
		t.Error("Get failed")
	}

	if res := tinyDB.SetNX([]byte("key1"), []byte("value2")); res != 0 {
		t.Error("SetNX failed")
	}
	if res := tinyDB.SetNX([]byte("key2"), []byte("value2")); res != 1 {
		t.Error("SetNX failed")
	}

	if res := tinyDB.MSetNX([]byte("key1"), []byte("value1"), []byte("key2"), []byte("value2"), []byte("key3"), []byte("value3")); res != 0 {
		t.Error("MSetNX failed")
	}
	if res := tinyDB.MSetNX([]byte("key3"), []byte("value3"), []byte("key4"), []byte("value4")); res != 1 {
		t.Error("MSetNX failed")
	}

	if res, _ := tinyDB.SetRange([]byte("key3"), []byte("suf"), 10); res != 13 {
		t.Error("SetRange failed")
	}
	if res, _ := tinyDB.SetRange([]byte("key5"), []byte("value5"), 0); res != 6 {
		t.Error("SetRange failed")
	}
	tinyDB.SetRange([]byte("key5"), []byte("b"), 1)
	if res, _ := tinyDB.Get([]byte("key5")); string(res) != "vblue5" {
		t.Error("SetRange failed")
	}
	tinyDB.SetRange([]byte("key5"), []byte("aaa"), 4)
	if res, _ := tinyDB.Get([]byte("key5")); string(res) != "vbluaaa" {
		t.Error("SetRange failed")
	}

	if res, _ := tinyDB.GetRange([]byte("key5"), 0, 3); string(res) != "vblu" {
		t.Error("GetRange failed")
	}
	if res, _ := tinyDB.GetRange([]byte("key5"), 0, 10); string(res) != "vbluaaa" {
		t.Error("GetRange failed")
	}
	if res, _ := tinyDB.GetRange([]byte("key5"), -2, -1); string(res) != "aa" {
		t.Error("GetRange failed")
	}

	if res, _ := tinyDB.Incr([]byte("key6"), 1); res != 1 {
		t.Error("Incr failed")
	}
	if res, _ := tinyDB.Incr([]byte("key6"), 1); res != 2 {
		t.Error("Incr failed")
	}

	if res, _ := tinyDB.IncrByFloat([]byte("key7"), 1.2); res != 1.2 {
		t.Error("IncrByFloat failed")
	}
	if res, _ := tinyDB.IncrByFloat([]byte("key7"), 1.2); res != 2.4 {
		t.Error("IncrByFloat failed")
	}

	if res, _ := tinyDB.Append([]byte("key5"), []byte("value5")); res != 13 {
		t.Error("Append failed")
	}

	if res := tinyDB.GetDel([]byte("key5")); res != "vbluaaavalue5" {
		t.Error("GetDel failed")
	}
	if res, _ := tinyDB.Get([]byte("key5")); res != nil {
		t.Error("GetDel failed")
	}
}

func BenchmarkStrWrite(b *testing.B) {
	_ = os.Setenv(constants.DebugEnv, "1")
	tinyDB := openDB(1 << 26) // 64M
	defer tinyDB.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = tinyDB.Set([]byte(fmt.Sprintf("key%v", i)), []byte("value"))
	}
}

func BenchmarkStrRead(b *testing.B) {
	_ = os.Setenv(constants.DebugEnv, "1")
	tinyDB := openDB(1 << 26) // 64M
	defer tinyDB.Close()
	for i := 0; i < b.N; i++ {
		_ = tinyDB.Set([]byte(fmt.Sprintf("key%v", i)), []byte("value"))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		x := rand.Intn(b.N)
		_, _ = tinyDB.Get([]byte(fmt.Sprintf("key%v", x)))
	}
}

func BenchmarkTinyDBWrite(b *testing.B) {
	rbd := redis.NewClient(&redis.Options{
		Addr: "localhost:6388",
	})
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rbd.Set(ctx, fmt.Sprintf("key%v", i), "value", 0)
	}
}

func BenchmarkRedisWrite(b *testing.B) {
	rbd := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rbd.Set(ctx, fmt.Sprintf("key%v", i), "value", 0)
	}
}

func BenchmarkTinyDBRead(b *testing.B) {
	rbd := redis.NewClient(&redis.Options{
		Addr: "localhost:6388",
	})
	ctx := context.Background()
	for i := 0; i < b.N; i++ {
		rbd.Set(ctx, fmt.Sprintf("key%v", i), "value", 0)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		x := rand.Intn(b.N)
		rbd.Get(ctx, fmt.Sprintf("key%v", x))
	}
}

func BenchmarkRedisRead(b *testing.B) {
	rbd := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	ctx := context.Background()
	for i := 0; i < b.N; i++ {
		rbd.Set(ctx, fmt.Sprintf("key%v", i), "value", 0)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		x := rand.Intn(b.N)
		rbd.Get(ctx, fmt.Sprintf("key%v", x))
	}
}

func BenchmarkParallelWrite(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		rbd := redis.NewClient(&redis.Options{
			Addr: "localhost:6388",
		})
		ctx := context.Background()
		i := 0
		for pb.Next() {
			rbd.Set(ctx, fmt.Sprintf("key%v", i), "value", 0)
			i++
		}
	})
}

func BenchmarkParallelWriteRedis(b *testing.B) {
	rbd := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	ctx := context.Background()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			rbd.Set(ctx, fmt.Sprintf("key%v", i), "value", 0)
			i++
		}
	})
}

func BenchmarkParallelRead(b *testing.B) {
	rbd := redis.NewClient(&redis.Options{
		Addr: "localhost:6388",
	})
	ctx := context.Background()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			rbd.Get(ctx, fmt.Sprintf("key%v", i))
			i++
		}
	})
}

func BenchmarkParallelReadRedis(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		rbd := redis.NewClient(&redis.Options{
			Addr: "localhost:6379",
		})
		ctx := context.Background()
		i := 0
		for pb.Next() {
			rbd.Get(ctx, fmt.Sprintf("key%v", i))
			i++
		}
	})
}

// 写入10w条数据
func TinyDBWrite(x int, wg *sync.WaitGroup) {
	N := 100000
	rbd := redis.NewClient(&redis.Options{
		Addr: "localhost:6388",
	})
	ctx := context.Background()
	for i := 0; i < N; i++ {
		rbd.Set(ctx, fmt.Sprintf("key%v", i+N*x), "value", 0)
	}
	wg.Done()
}

func Test_Consistency(t *testing.T) {
	wg := &sync.WaitGroup{}
	// 100个客户端
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go TinyDBWrite(i, wg)
	}
	wg.Wait()

	rbd := redis.NewClient(&redis.Options{
		Addr: "localhost:6388",
	})
	ctx := context.Background()
	// 检查数据一致性
	for i := 0; i < 100; i++ {
		for j := 0; j < 100000; j++ {
			if rbd.Get(ctx, fmt.Sprintf("key%v", i+100000*i)).Val() != "value" {
				t.Log("key: ", fmt.Sprintf("key%v", i+100000*i))
				t.Error("Consistency error")
			}
		}
	}
}
