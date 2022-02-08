package ankrscan

import (
	"context"
	"encoding/hex"
	"fmt"
	"github.com/Ankr-network/ankrscan-proto-contract/go/proto"
	"github.com/stretchr/testify/require"
	"log"
	"testing"
)

func TestConsumer(t *testing.T) {
	sdk, err := NewSdk("http://65.108.100.113")
	require.NoError(t, err)
	blockProcessor := &TestBlockProcessor{miners: make(map[string]int)}
	progressReporter := &TestProgressReporter{}
	consumer := sdk.NewConsumer(&ConsumerConfig{ConsumerId: "filtering-consumer-0", BlockchainId: "BSC", BatchSize: 50, StartBlock: 100, BlockProcessor: blockProcessor, ProgressReporter: progressReporter})
	ctx := context.Background()
	for {
		err := consumer.Process(ctx)
		require.NoError(t, err)
	}
}

func TestLatest(t *testing.T) {
	sdk, err := NewSdk("http://65.108.100.113")
	require.NoError(t, err)
	header, err := sdk.LatestBlockHeader(context.Background(), "BSC")
	require.NoError(t, err)
	log.Println(header.BlockHashAsHash().String())
}

type TestBlockProcessor struct {
	miners map[string]int
}

func (s *TestBlockProcessor) Process(blocks []*proto.Block, isReorg bool) error {
	for _, block := range blocks {
		value := 1
		if isReorg {
			value = -1
		}
		s.miners[hex.EncodeToString(block.Header.GetEthBlock().Miner)] += value
	}
	log.Println("miners", s.miners)
	return nil
}

type TestProgressReporter struct {
}

func (s *TestProgressReporter) Report(blocks []*proto.Block, isReorg bool, timings *Timings, lag uint64, latest *proto.BlockHeader) error {
	log.Println(fmt.Sprintf(DefaultReport(blocks, isReorg, timings, lag, latest)))
	return nil
}
