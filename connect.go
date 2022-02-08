package ankrscan

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"io"
	"net/http"
)

type errorMessage struct {
	Message string `json:"message"`
}

func Dial(url string) (*HttpClientConn, error) {
	return &HttpClientConn{client: http.Client{}, url: url}, nil
}

type HttpClientConn struct {
	client http.Client
	url    string
}

var method2url = map[string]string{
	"/ankrscan.blockstore.BlockStore/Next": "/v1/multichain/consumer/next",
	"/ankrscan.blockstore.BlockStore/Commit": "/v1/multichain/consumer/commit",
	"/ankrscan.blockstore.BlockStore/Seek": "/v1/multichain/consumer/seek",
	"/ankrscan.blockstore.BlockStore/LastCommit": "/v1/multichain/consumer/latest",
	"/ankrscan.blockstore.BlockStore/BlocksByNumber": "/v1/multichain/block/byNumber",
	"/ankrscan.blockstore.BlockStore/LatestBlockHeader": "/v1/multichain/block/latest",
	"/ankrscan.blockstore.BlockStore/BlockRangeContinuous": "/v1/multichain/block/range",
}

func (cc *HttpClientConn) Invoke(ctx context.Context, method string, args interface{}, reply interface{}, opts ...grpc.CallOption) error {
	marshalled, err := protojson.Marshal(args.(proto.Message))
	if err != nil {
		return err
	}
	methodUrl, ok := method2url[method]
	if !ok {
		return errors.Errorf("unknown method %s", method)
	}
	url := fmt.Sprintf("%s%s", cc.url, methodUrl)
	request, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(marshalled))
	if err != nil {
		return err
	}
	request.Header.Add("Content-Type", "application/json")
	response, err := cc.client.Do(request)
	if err != nil {
		return err
	}
	responseBody, err := io.ReadAll(response.Body)
	if err != nil {
		return err
	}
	if response.StatusCode != 200 {
		message := &errorMessage{}
		if err = json.Unmarshal(responseBody, message); err != nil {
			return errors.Errorf("http request to %s failed with %s: %s", url, response.Status, string(responseBody))
		}
		return errors.Errorf(message.Message)
	}
	if err = protojson.Unmarshal(responseBody, reply.(proto.Message)); err != nil {
		return err
	}
	return nil
}

func (cc *HttpClientConn) NewStream(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	return nil, nil
}
