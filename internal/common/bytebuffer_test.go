package common_test

import (
	"io/ioutil"
	"testing"

	"github.com/bsElyes/rsocket-go/internal/common"
	"github.com/stretchr/testify/assert"
)

func TestByteBuff_Bytes(t *testing.T) {
	data := []byte("foobar")
	b := common.BorrowByteBuff()
	defer common.ReturnByteBuff(b)
	wrote, err := b.Write(data)
	assert.NoError(t, err, "write failed")
	assert.Equal(t, len(data), wrote, "wrong wrote size")
	assert.Equal(t, data, b.Bytes(), "wrong data")
}

//func TestByteBuff_WriteUint24(t *testing.T) {
//	b := common.BorrowByteBuff()
//	defer common.ReturnByteBuff(b)
//	var err error
//	err = b.WriteUint24(0)
//	assert.NoError(t, err, "write uint24 failed")
//	err = b.WriteUint24(u24.MaxUint24)
//	assert.NoError(t, err, "write maximum uint24 failed")
//	err = b.WriteUint24(0x01FFFFFF)
//	assert.Error(t, err, "should write failed")
//}
//
//func TestByteBuff_Len(t *testing.T) {
//	b := common.BorrowByteBuff()
//	defer common.ReturnByteBuff(b)
//	// 3+1+6
//	_ = b.WriteUint24(1)
//	_ = b.WriteByte('c')
//	_, _ = b.Write([]byte("foobar"))
//	assert.Equal(t, 10, b.Len(), "wrong length")
//}

func TestByteBuff_WriteTo(t *testing.T) {
	b := common.BorrowByteBuff()
	defer common.ReturnByteBuff(b)
	// 1MB
	s := common.RandAlphanumeric(1 * 1024 * 1024)
	err := b.WriteString(s)
	assert.NoError(t, err)
	n, err := b.WriteTo(ioutil.Discard)
	assert.NoError(t, err, "WriteTo failed")
	assert.Equal(t, len(s), int(n), "wrong length")
}
