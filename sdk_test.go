package ankrscan

import (
	"context"
	"encoding/hex"
	"fmt"
	proto "github.com/Ankr-network/ankrscan-proto-contract"
	"github.com/stretchr/testify/require"
	"log"
	"testing"
)

func TestNewSdk(t *testing.T) {
	sdk, err := NewSdk("https://localhost:8058/ankrscan/0367deca75d17c37af98e9dd9130a47c1e4a8a5a1e562ff8f179d36018dc1b58")
	require.NoError(t, err)
	require.NotNil(t, sdk)
	require.Equal(t, "0367deca75d17c37af98e9dd9130a47c1e4a8a5a1e562ff8f179d36018dc1b58", sdk.Token())
}

func TestConsumer(t *testing.T) {
	sdk := NewSdkFromToken("localhost:8058", "0367deca75d17c37af98e9dd9130a47c1e4a8a5a1e562ff8f179d36018dc1b58")
	blockProcessor := &TestBlockProcessor{miners: make(map[string]int)}
	progressReporter := &TestProgressReporter{}
	consumer := sdk.NewConsumer(&ConsumerConfig{ConsumerId: "filtering-consumer-0", BlockchainId: "eth", BatchSize: 50, StartBlock: 14076289 - 5000, BlockProcessor: blockProcessor, ProgressReporter: progressReporter})
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