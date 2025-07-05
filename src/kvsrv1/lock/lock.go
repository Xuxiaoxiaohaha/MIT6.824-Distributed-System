package lock

import (
	"fmt"
	"os"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	lockKey    string // 如果我需要对KV中key为"xqx"的进行操作时，我只需要"lock_xqx"锁即可
	lockClient string // 对应的client，后续解锁时需要提供这个，防止他人误解
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	// You may add code here
	lockKey := "lock_" + l
	lk := &Lock{ck: ck, lockKey: lockKey, lockClient: kvtest.RandValue(8)}
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	for {
		value, version, err := lk.ck.Get(lk.lockKey)
		if err == rpc.ErrNoKey || (err == rpc.OK && value == "0") { // 未上锁
			// 用Version来保证原子性（当两个client同时看到）锁未被持有，然后同时发起PUT请求持锁，
			// 此时就需要version来确保只能有一个client请求到锁
			err = lk.ck.Put(lk.lockKey, lk.lockClient, version)
			if err == rpc.OK { // 成功请求到锁
				// log.Printf("client %s has acquired lock %s \n", lk.lockClient, lk.lockKey)
				return
			}
			if err == rpc.ErrVersion { // 未请求到锁
				continue
			}
			if err == rpc.ErrNoKey { // 不可能发生的情况
				// log.Printf("不可能发生的情况 \n")
				return
			}
			if err == rpc.ErrMaybe { // 不确定是否请求到锁,因此需要lockClient来进一步确定
				value, _, err = lk.ck.Get(lk.lockKey)
				if err == rpc.OK && value == lk.lockClient {
					return
				}
			}
		}
		// log.Printf("client %s does not acquire lock %s \n", lk.lockClient, lk.lockKey)
		// time.Sleep(500 * time.Millisecond)
	}
}

func (lk *Lock) Release() {
	// Your code here
	value, version, err := lk.ck.Get(lk.lockKey)
	if err == rpc.OK && value == lk.lockClient {
		lk.ck.Put(lk.lockKey, "0", version)
	} else {
		fmt.Println("Some error occurred")
		os.Exit(1)
	}
}
