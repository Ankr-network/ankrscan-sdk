package ankrscan

import (
	"context"
	"github.com/Ankr-network/ankrscan-proto-contract/go/proto"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	neturl "net/url"
	"path"
)

type Sdk struct {
	url              string
	token            string
	isGrpc           bool
	blockStoreClient proto.BlockStoreClient
}

func NewSdk(url string) (*Sdk, error) {
	parsedUrl, err := neturl.Parse(url)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to parse url")
	}
	_, token := path.Split(parsedUrl.Path)
	sdk := &Sdk{url: url, token: token}
	return sdk, nil
}

func NewGrpcSdk(url string) (*Sdk, error) {
	return &Sdk{url: url, isGrpc: true}, nil
}

func (s *Sdk) Token() string {
	return s.token
}

func (s *Sdk) Url() string {
	return s.url
}

func (s *Sdk) dial() (grpc.ClientConnInterface, error) {
	if s.isGrpc {
		return grpc.Dial(s.url, grpc.WithInsecure(), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(104857600)))
	} else {
		return Dial(s.url)
	}
}

func (s *Sdk) BlockStoreClient() (proto.BlockStoreClient, error) {
	if s.blockStoreClient == nil {
		clientConn, err := s.dial()
		if err != nil {
			return nil, errors.Wrapf(err, "failed to connect to block store")
		}
		s.blockStoreClient = proto.NewBlockStoreClient(clientConn)
	}
	return s.blockStoreClient, nil
}

func (s *Sdk) NewConsumer(config *ConsumerConfig) *Consumer {
	return &Consumer{
		config: config,
		sdk:    s,
	}
}

func (s *Sdk) LatestBlockHeader(ctx context.Context, blockchainId string) (*proto.BlockHeader, error) {
	client, err := s.BlockStoreClient()
	if err != nil {
		return nil, err
	}
	reply, err := client.LatestBlockHeader(ctx, &proto.LatestBlockHeaderRequest{
		BlockchainName: blockchainId,
	})
	if err != nil {
		return nil, err
	}
	return reply.Block, nil
}
