package socket_test

import (
	"testing"
	"time"

	"github.com/bsElyes/rsocket-go/core"
	"github.com/bsElyes/rsocket-go/core/transport"
	"github.com/bsElyes/rsocket-go/internal/socket"
	"github.com/bsElyes/rsocket-go/logger"
	"github.com/golang/mock/gomock"
	"github.com/pkg/errors"
)

var (
	fakeErr = errors.New("fake error")

	fakeMetadata = []byte("fake-metadata")
	fakeData     = []byte("fake-data")
	fakeMimeType = []byte("fake-mime-type")

	fakeSetup = &socket.SetupInfo{
		Version:           core.DefaultVersion,
		MetadataMimeType:  fakeMimeType,
		DataMimeType:      fakeMimeType,
		Metadata:          fakeMetadata,
		Data:              fakeData,
		KeepaliveLifetime: 90 * time.Second,
		KeepaliveInterval: 30 * time.Second,
	}
)

func InitTransportWithController(ctrl *gomock.Controller) (*MockConn, *transport.Transport) {
	conn := NewMockConn(ctrl)
	tp := transport.NewTransport(conn)
	return conn, tp
}

func InitTransport(t *testing.T) (*gomock.Controller, *MockConn, *transport.Transport) {
	ctrl := gomock.NewController(t)
	conn, tp := InitTransportWithController(ctrl)
	return ctrl, conn, tp
}

func init() {
	logger.SetLevel(logger.LevelError)
}
