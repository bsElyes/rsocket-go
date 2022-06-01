package fragmentation

import (
	"testing"

	"github.com/bsElyes/rsocket-go/core"
	"github.com/bsElyes/rsocket-go/core/framing"
	"github.com/bsElyes/rsocket-go/internal/common"
	"github.com/stretchr/testify/assert"
)

func TestSplitter_Split(t *testing.T) {
	const mtu = 128
	data := []byte(common.RandAlphanumeric(1024))
	metadata := []byte(common.RandAlphanumeric(512))

	joiner, err := split2joiner(mtu, data, metadata)
	assert.NoError(t, err, "split failed")

	m, ok := joiner.Metadata()
	assert.True(t, ok, "bad metadata")
	assert.Equal(t, metadata, m, "bad metadata")
	assert.Equal(t, data, joiner.Data(), "bad data")
}

func split2joiner(mtu int, data, metadata []byte) (joiner Joiner, err error) {
	fn := func(idx int, result SplitResult) {
		sid := uint32(77778888)
		if idx == 0 {
			f := framing.NewWriteablePayloadFrame(sid, result.Data, result.Metadata, core.FlagComplete|result.Flag)
			joiner = NewJoiner(f)
		} else {
			f := framing.NewWriteablePayloadFrame(sid, result.Data, result.Metadata, result.Flag)
			joiner.Push(f)
		}
	}
	Split(mtu, data, metadata, fn)
	return
}
