package http_storage

import (
	"context"
	"testing"

	"github.com/storage-lock/go-storage"
	storage_lock "github.com/storage-lock/go-storage-lock"
	storage_test_helper "github.com/storage-lock/go-storage-test-helper"
	"github.com/stretchr/testify/assert"
)

func TestHTTPStorage(t *testing.T) {
	s, err := NewHTTPStorage(context.Background())
	if err != nil {
		t.Fatalf("创建 HTTP 存储失败: %v", err)
	}
	storage_test_helper.TestStorage(t, s)
}

func TestHTTPStorageCapabilities(t *testing.T) {
	s, err := NewHTTPStorage(context.Background())
	assert.Nil(t, err)
	caps := s.Capabilities()
	assert.Contains(t, caps, storage.CapabilityCAS)
	assert.Contains(t, caps, storage.CapabilityReliableTime)
	assert.Contains(t, caps, storage.CapabilityAtomicDelete)
}

func TestHTTPStorageErrorBranches(t *testing.T) {
	ctx := context.Background()
	s, err := NewHTTPStorage(ctx)
	assert.Nil(t, err)
	info := storage_test_helper.BuildTestLockInformation(t)

	// Create 重复 -> ErrLockAlreadyExists
	assert.Nil(t, s.CreateWithVersion(ctx, "dup-lock", 1, info))
	assert.ErrorIs(t, s.CreateWithVersion(ctx, "dup-lock", 1, info), storage_lock.ErrLockAlreadyExists)

	// Update 不存在 -> ErrVersionMiss（可重试）；版本不对 -> ErrVersionMiss
	assert.ErrorIs(t, s.UpdateWithVersion(ctx, "no-such", 1, 2, info), storage_lock.ErrVersionMiss)
	assert.ErrorIs(t, s.UpdateWithVersion(ctx, "dup-lock", 999, 1000, info), storage_lock.ErrVersionMiss)

	// Delete 不存在 -> ErrLockNotFound；版本不对 -> ErrVersionMiss
	assert.ErrorIs(t, s.DeleteWithVersion(ctx, "no-such", 1, info), storage_lock.ErrLockNotFound)
	assert.ErrorIs(t, s.DeleteWithVersion(ctx, "dup-lock", 999, info), storage_lock.ErrVersionMiss)

	// Get 不存在 -> ErrLockNotFound
	_, err = s.Get(ctx, "no-such")
	assert.ErrorIs(t, err, storage_lock.ErrLockNotFound)

	// 正常 Update + Delete 走通
	assert.Nil(t, s.UpdateWithVersion(ctx, "dup-lock", 1, 2, info))
	assert.Nil(t, s.DeleteWithVersion(ctx, "dup-lock", 2, info))
}

func TestHTTPStorageListInvalidJson(t *testing.T) {
	ctx := context.Background()
	s, err := NewHTTPStorage(ctx)
	assert.Nil(t, err)
	// 直接塞一条坏数据，覆盖 List 的 json 解析失败分支
	s.mu.Lock()
	s.storageMap["bad-lock"] = &httpStorageValue{Version: 1, LockInformationJsonString: "{invalid-json"}
	s.mu.Unlock()
	_, err = s.List(ctx)
	assert.NotNil(t, err)
}

func TestHTTPStorageWatch(t *testing.T) {
	s, err := NewHTTPStorage(context.Background())
	assert.Nil(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, err := s.Watch(ctx, "watched")
	assert.Nil(t, err)
	assert.NotNil(t, ch)
	_, err = s.Watch(ctx, "")
	assert.NotNil(t, err)
}
