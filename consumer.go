package ankrscan

import (
	"context"
	"fmt"
	proto "github.com/Ankr-network/ankrscan-proto-contract/generated/go"
	"math"
	"time"
)

type Consumer struct {
	config      *ConsumerConfig
	sdk         *Sdk
	initialized bool
}

const StartBlockOrigin = 0
const StartBlockLatest = math.MaxUint64

type BlockProcessor interface {
	Process(blocks []*proto.Block, isReorg bool) error
}

type Timings struct {
	fetch   time.Duration
	process time.Duration
	commit  time.Duration
}

func (t *Timings) FetchMs() int64 {
	return int64(t.fetch) / 1e6
}

func (t *Timings) ProcessMs() int64 {
	return int64(t.process) / 1e6
}

func (t *Timings) CommitMs() int64 {
	return int64(t.commit) / 1e6
}

func (t *Timings) Total() time.Duration {
	return t.fetch + t.process + t.commit
}

type ProgressReporter interface {
	Report(blocks []*proto.Block, isReorg bool, timings *Timings, lag uint64, latest *proto.BlockHeader) error
}

func DefaultReport(blocks []*proto.Block, isReorg bool, timings *Timings, lag uint64, latest *proto.BlockHeader) string {
	reorgString := ""
	if isReorg {
		reorgString = "reorged"
	}
	return fmt.Sprintf("processed %d %s blocks, lag is %d blocks, fetched in %d ms, processed in %d ms, committed in %d ms", len(blocks), reorgString, lag, timings.FetchMs(), timings.ProcessMs(), timings.CommitMs())
}

type ConsumerConfig struct {
	ConsumerId       string
	BlockchainId     string
	BatchSize        uint64
	StartBlock       uint64 // can be StartBlockOrigin or StartBlockLatest or any other offset
	BlockProcessor   BlockProcessor
	ProgressReporter ProgressReporter
}

func (c *Consumer) Proto() *proto.BlockConsumer {
	return &proto.BlockConsumer{
		ConsumerName:   c.Config().ConsumerId,
		UserId:         c.Sdk().Token(),
		BlockchainName: c.Config().BlockchainId,
	}
}

func (c *Consumer) Config() *ConsumerConfig {
	return c.config
}

func (c *Consumer) Sdk() *Sdk {
	return c.sdk
}

func (c *Consumer) Exists(ctx context.Context) (bool, error) {
	lastCommit, err := c.LastCommit(ctx)
	if err != nil {
		return false, err
	}
	return lastCommit.Exists, nil
}

func (c *Consumer) Next(ctx context.Context) (*proto.NextReply, error) {
	if c.initialized == false {
		exists, err := c.Exists(ctx)
		if err != nil {
			return nil, err
		}
		if !exists {
			startBlock := c.Config().StartBlock
			if startBlock == StartBlockLatest {
				latest, err := c.Sdk().LatestBlockHeader(ctx, c.Config().BlockchainId)
				if err != nil {
					return nil, err
				}
				startBlock = latest.BlockHeight - 1
			}
			if err = c.Seek(ctx, startBlock); err != nil {
				return nil, err
			}
		}
	}
	c.initialized = true
	request := &proto.NextRequest{
		Consumer:  c.Proto(),
		BatchSize: c.Config().BatchSize,
	}
	client, err := c.Sdk().BlockStoreClient()
	if err != nil {
		return nil, err
	}
	reply, err := client.Next(ctx, request)
	if err != nil {
		return nil, err
	}
	return reply, nil
}

func (c *Consumer) Commit(ctx context.Context, commit *proto.BlockHeader) error {
	request := &proto.CommitRequest{
		Consumer: c.Proto(),
		Block:    commit,
	}
	client, err := c.Sdk().BlockStoreClient()
	if err != nil {
		return err
	}
	_, err = client.Commit(ctx, request)
	if err != nil {
		return err
	}
	return nil
}

func (c *Consumer) Seek(ctx context.Context, blockHeight uint64) error {
	request := &proto.SeekRequest{
		Consumer:    c.Proto(),
		BlockHeight: blockHeight,
	}
	client, err := c.Sdk().BlockStoreClient()
	if err != nil {
		return err
	}
	_, err = client.Seek(ctx, request)
	if err != nil {
		return err
	}
	return nil
}

func (c *Consumer) LastCommit(ctx context.Context) (*proto.LastCommitReply, error) {
	request := &proto.LastCommitRequest{
		Consumer: c.Proto(),
	}
	client, err := c.Sdk().BlockStoreClient()
	if err != nil {
		return nil, err
	}
	reply, err := client.LastCommit(ctx, request)
	if err != nil {
		return nil, err
	}
	return reply, nil
}

func (c *Consumer) Process(ctx context.Context) ([]*proto.Block, error) {
	timings := &Timings{}
	start := time.Now().UnixNano()
	reply, err := c.Next(ctx)
	if err != nil {
		return nil, err
	}
	if len(reply.Blocks) == 0 {
		time.Sleep(time.Second)
		return reply.Blocks, nil
	}
	timings.fetch = time.Duration(time.Now().UnixNano() - start)
	start = time.Now().UnixNano()
	if err = c.Config().BlockProcessor.Process(reply.Blocks, reply.IsReorg); err != nil {
		return nil, err
	}
	timings.process = time.Duration(time.Now().UnixNano() - start)
	start = time.Now().UnixNano()
	if err = c.Commit(ctx, reply.Commit); err != nil {
		return nil, err
	}
	timings.commit = time.Duration(time.Now().UnixNano() - start)
	if c.Config().ProgressReporter != nil {
		if err = c.Config().ProgressReporter.Report(reply.Blocks, reply.IsReorg, timings, reply.Latest.BlockHeight-reply.Commit.BlockHeight, reply.Latest); err != nil {
			return nil, err
		}
	}
	return reply.Blocks, nil
}
