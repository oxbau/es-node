// Copyright 2022-2023, es.
// For license information, see https://github.com/ethstorage/es-node/blob/main/LICENSE

package blobs

import (
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
	es "github.com/ethstorage/go-ethstorage/ethstorage"
)

type BlobCacheReader interface {
	GetKeyValueByIndex(index uint64, hash common.Hash) []byte
	GetKeyValueByIndexUnchecked(index uint64) []byte
}

// BlobReader provides unified interface for the miner to read blobs and samples
// from StorageManager and downloader cache.
type BlobReader struct {
	cr BlobCacheReader
	sm *es.StorageManager
	lg log.Logger
}

func NewBlobReader(cr BlobCacheReader, sm *es.StorageManager, lg log.Logger) *BlobReader {
	return &BlobReader{
		cr: cr,
		sm: sm,
		lg: lg,
	}
}

func (n *BlobReader) GetBlob(kvIdx uint64, kvHash common.Hash) ([]byte, error) {
	blob := n.cr.GetKeyValueByIndex(kvIdx, kvHash)
	if blob != nil {
		n.lg.Debug("Loaded blob from downloader cache", "kvIdx", kvIdx)
		return blob, nil
	}
	blob, exist, err := n.sm.TryRead(kvIdx, int(n.sm.MaxKvSize()), kvHash)
	if err != nil {
		return nil, err
	}
	if !exist {
		return nil, fmt.Errorf("kv not found: index=%d", kvIdx)
	}
	n.lg.Debug("Loaded blob from storage manager", "kvIdx", kvIdx)
	return blob, nil
}

func (n *BlobReader) ReadSample(shardIdx, sampleIdx uint64) (common.Hash, error) {
	sampleLenBits := n.sm.MaxKvSizeBits() - es.SampleSizeBits
	kvIdx := sampleIdx >> sampleLenBits

	if blob := n.cr.GetKeyValueByIndexUnchecked(kvIdx); blob != nil {
		n.lg.Debug("Loaded blob from downloader cache", "kvIdx", kvIdx)
		sampleIdxInKv := sampleIdx % (1 << sampleLenBits)
		sampleSize := uint64(1 << es.SampleSizeBits)
		sampleIdxByte := sampleIdxInKv << es.SampleSizeBits
		sample := blob[sampleIdxByte : sampleIdxByte+sampleSize]
		return common.BytesToHash(sample), nil
	}

	encodedSample, err := n.sm.ReadSampleUnlocked(shardIdx, sampleIdx)
	if err != nil {
		return common.Hash{}, err
	}
	return encodedSample, nil
}
