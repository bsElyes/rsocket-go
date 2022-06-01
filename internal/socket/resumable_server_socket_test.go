package socket_test

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/bsElyes/rsocket-go/core"
	"github.com/bsElyes/rsocket-go/core/framing"
	"github.com/bsElyes/rsocket-go/internal/fragmentation"
	"github.com/bsElyes/rsocket-go/internal/socket"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

var fakeToken = []byte("fakeToken")

func TestResumableServerSocket_Start(t *testing.T) {
	ctrl, conn, tp := InitTransport(t)
	defer ctrl.Finish()

	// For test
	readChan := make(chan core.BufferedFrame, 64)
	setupFrame := framing.NewSetupFrame(
		core.DefaultVersion,
		30*time.Second,
		90*time.Second,
		nil,
		fakeMimeType,
		fakeMimeType,
		fakeData,
		fakeMetadata,
		false,
	)
	readChan <- setupFrame

	conn.EXPECT().Close().Times(1)
	conn.EXPECT().SetCounter(gomock.Any()).AnyTimes()
	conn.EXPECT().Write(gomock.Any()).Return(nil).AnyTimes()
	conn.EXPECT().Flush().AnyTimes()
	conn.EXPECT().Read().DoAndReturn(func() (core.BufferedFrame, error) {
		next, ok := <-readChan
		if !ok {
			return nil, io.EOF
		}
		return next, nil
	}).AnyTimes()
	conn.EXPECT().SetDeadline(gomock.Any()).AnyTimes()

	firstFrame, err := tp.ReadFirst(context.Background())
	assert.NoError(t, err, "read first frame failed")
	assert.Equal(t, setupFrame, firstFrame, "first should be setup frame")

	close(readChan)

	c := socket.NewServerDuplexConnection(context.Background(), nil, nil, fragmentation.MaxFragment, nil)
	ss := socket.NewResumableServerSocket(c, fakeToken)

	ss.SetResponder(fakeResponder)
	ss.SetTransport(tp)

	token, ok := ss.Token()
	assert.True(t, ok)
	assert.Equal(t, fakeToken, token, "token doesn't match")

	done := make(chan struct{})
	go func() {
		defer close(done)
		err := ss.Start(context.Background())
		assert.NoError(t, err, "start server socket failed")
	}()

	err = tp.Start(context.Background())
	assert.NoError(t, err, "start transport failed")

	_ = c.Close()

	assert.Equal(t, true, ss.Pause(), "should return true")

	<-done
}
