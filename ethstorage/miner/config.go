// Copyright 2022-2023, EthStorage.
// For license information, see https://github.com/ethstorage/es-node/blob/main/LICENSE

package miner

import (
	"math/big"
	"path/filepath"
	"runtime"

	opcrypto "github.com/ethereum-optimism/optimism/op-service/crypto"
	"github.com/ethereum/go-ethereum/common"
)

type Config struct {
	// contract
	RandomChecks   uint64
	NonceLimit     uint64
	StartTime      uint64
	ShardEntry     uint64
	TreasuryShare  uint64
	MinimumDiff    *big.Int
	Cutoff         *big.Int
	DiffAdjDivisor *big.Int
	StorageCost    *big.Int
	PrepaidAmount  *big.Int
	DcfFactor      *big.Int

	// cli
	GasPrice         *big.Int
	PriorityGasPrice *big.Int
	ZKeyFileName     string
	ZKWorkingDir     string
	ZKProverMode     uint64
	ZKProverImpl     uint64
	ThreadsPerShard  uint64
	SignerFnFactory  opcrypto.SignerFactory
	SignerAddr       common.Address
	MinimumProfit    *big.Int
}

var DefaultConfig = Config{
	RandomChecks:   2,
	NonceLimit:     1048576,
	MinimumDiff:    new(big.Int).SetUint64(1000000),
	Cutoff:         new(big.Int).SetUint64(60),
	DiffAdjDivisor: new(big.Int).SetUint64(1024),

	GasPrice:         nil,
	PriorityGasPrice: nil,
	ZKeyFileName:     "blob_poseidon2.zkey",
	ZKWorkingDir:     filepath.Join("build", "bin"),
	ZKProverMode:     2,
	ZKProverImpl:     1,
	ThreadsPerShard:  uint64(2 * runtime.NumCPU()),
	MinimumProfit:    common.Big0,
}
