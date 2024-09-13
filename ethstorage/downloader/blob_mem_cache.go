// Copyright 2022-2023, EthStorage.
// For license information, see https://github.com/ethstorage/es-node/blob/main/LICENSE

package downloader

import (
	"bytes"
	"sync"

	"github.com/ethereum/go-ethereum/common"

	"github.com/ethstorage/go-ethstorage/ethstorage"
)

type BlobMemCache struct {
	blocks map[uint64]*blockBlobs
	mu     sync.RWMutex
}

func NewBlobMemCache() *BlobMemCache {
	return &BlobMemCache{
		blocks: map[uint64]*blockBlobs{},
	}
}

func (c *BlobMemCache) SetBlockBlobs(block *blockBlobs) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.blocks[block.number] = block
	return nil
}

func (c *BlobMemCache) Blobs(number uint64) []blob {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if _, exist := c.blocks[number]; !exist {
		return nil
	}

	res := []blob{}
	for _, blob := range c.blocks[number].blobs {
		res = append(res, *blob)
	}
	return res
}

func (c *BlobMemCache) GetKeyValueByIndex(idx uint64, hash common.Hash) []byte {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for _, block := range c.blocks {
		for _, blob := range block.blobs {
			if blob.kvIndex.Uint64() == idx && bytes.Equal(blob.hash[0:ethstorage.HashSizeInContract], hash[0:ethstorage.HashSizeInContract]) {
				return blob.data
			}
		}
	}
	return nil
}

func (c *BlobMemCache) GetSampleData(idx, sampleIdxInKv uint64) []byte {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for _, block := range c.blocks {
		for _, blob := range block.blobs {
			if blob.kvIndex.Uint64() == idx {
				sampleSize := uint64(1 << ethstorage.SampleSizeBits)
				sampleIdxByte := sampleIdxInKv << ethstorage.SampleSizeBits
				sample := blob.data[sampleIdxByte : sampleIdxByte+sampleSize]
				return sample
			}
		}
	}
	return nil
}

// TODO: @Qiang An edge case that may need to be handled when Ethereum block is NOT finalized for a long time
// We may need to add a counter in SetBlockBlobs(), if the counter is greater than a threshold which means
// there has been a long time after last Cleanup, so we need to Cleanup anyway in SetBlockBlobs.
func (c *BlobMemCache) Cleanup(finalized uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for hash, block := range c.blocks {
		if block.number <= finalized {
			delete(c.blocks, hash)
		}
	}
}

func (c *BlobMemCache) Close() error {
	c.blocks = nil
	return nil
}
