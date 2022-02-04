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
	sdk := NewSdk("localhost:6565", "")
	blockProcessor := &TestBlockProcessor{miners: make(map[string]int)}
	progressReporter := &TestProgressReporter{}
	consumer := sdk.NewConsumer(&ConsumerConfig{ConsumerId: "filtering-consumer-0", BlockchainId: "eth", BatchSize: 50, StartBlock: 14000144, BlockProcessor: blockProcessor, ProgressReporter: progressReporter})
	ctx := context.Background()
	for {
		err := consumer.Process(ctx)
		require.NoError(t, err)
	}
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
