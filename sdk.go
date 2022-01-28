package ankrscan

import (
	"context"
	proto "github.com/Ankr-network/ankrscan-proto-contract"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	neturl "net/url"
	"path"
)

type Sdk struct {
	url              string
	token            string
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

func NewSdkFromToken(url string, token string) *Sdk {
	return &Sdk{url: url, token: token}
}

func (s *Sdk) Token() string {
	return s.token
}

func (s *Sdk) Url() string {
	return s.url
}

func (s *Sdk) BlockStoreClient() (proto.BlockStoreClient, error) {
	if s.blockStoreClient == nil {
		conn, err := grpc.Dial(s.url, grpc.WithInsecure(), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(104857600)))
		if err != nil {
			return nil, errors.Wrapf(err, "failed to connect to block store")
		}
		s.blockStoreClient = proto.NewBlockStoreClient(conn)
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
