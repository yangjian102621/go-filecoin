package net

import (
	"context"

	"gx/ipfs/QmR8BauakNcBa3RbE4nbQu76PDiJgoQgz8AJdhJuiU4TAw/go-cid"
	"gx/ipfs/QmVmDhyTTUcQXFD1rRQ64fGLMSAoaQvNH3hwuaCFAPq2hy/errors"
	"gx/ipfs/QmWoXtvgC8inqFkAATB7cp2Dax7XBi9VDvSg9RCCZufmRk/go-block-format"
	"gx/ipfs/QmXixGGfd98hN2dA5YiPHWANY3sjmHfZBQk3mLiQUo6NLJ/go-bitswap"
	bserv "gx/ipfs/QmZsGVGCqMCNzHLNMB6q4F6yyvomqf1VxwhJwSfgo1NGaF/go-blockservice"
	logging "gx/ipfs/QmbkT7eMTyXfpeyB3ZMxxcxg7XH8t6uXp49jqzz4HB7BGF/go-log"

	"github.com/filecoin-project/go-filecoin/types"
)

var logFetcher = logging.Logger("net.fetcher")

// Fetcher is used to fetch data over the network.  It is implemented with
// a networked blockservice and a persistent bitswap session.
type Fetcher struct {
	// blockService is a network enabled blockstore.
	blockService bserv.BlockService

	// session is a bitswap session that enables efficient transfer.
	session *bserv.Session
}

// NewFetcher returns a Fetcher wired up to the input BlockService and a newly
// initialized persistent session of the block service.
func NewFetcher(ctx context.Context, bsrv bserv.BlockService) *Fetcher {
	return &Fetcher{
		blockService: bsrv,
		session:      bserv.NewSession(ctx, bsrv),
	}
}

// GetBlocks fetches the blocks with the given cids from the network using the
// Fetcher's bitswap session.
func (f *Fetcher) GetBlocks(ctx context.Context, cids []cid.Cid) ([]*types.Block, error) {
	var unsanitized []blocks.Block
	for b := range f.session.GetBlocks(ctx, cids) {
		unsanitized = append(unsanitized, b)
	}

	if len(unsanitized) < len(cids) {
		return nil, errors.Wrap(ctx.Err(), "failed to fetch all requested blocks")
	}

	var blocks []*types.Block
	for _, u := range unsanitized {
		block, err := types.DecodeBlock(u.RawData())
		if err != nil {
			return nil, errors.Wrap(err, "returned data was not a block")
		}
		blocks = append(blocks, block)
	}
	return blocks, nil
}

// LogStat logs the Fetchers bitswap session statistics
func (f *Fetcher) LogStat() {
	if f.blockService == nil {
		logFetcher.Errorf("attempt to read stats without init")
	}

	stat, err := f.blockService.Exchange().(*bitswap.Bitswap).Stat()
	if err != nil {
		logFetcher.Errorf("problem trying to read stats : %s", err.Error())
	}
	logFetcher.Debugf("duplicate downloaded blocks: %d", stat.DupBlksReceived)
	logFetcher.Debugf("total data downloaded: %d", stat.DataReceived)
	logFetcher.Debugf("total num objects downloaded: %d", stat.BlocksReceived)
}
