package flux_test

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/bsElyes/rsocket-go/payload"
	"github.com/bsElyes/rsocket-go/rx"
	"github.com/bsElyes/rsocket-go/rx/flux"
	"github.com/jjeffcaii/reactor-go"
	reactorFlux "github.com/jjeffcaii/reactor-go/flux"
	"github.com/jjeffcaii/reactor-go/scheduler"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"go.uber.org/atomic"
)

func TestEmpty(t *testing.T) {
	last, err := flux.Empty().
		DoOnNext(func(input payload.Payload) error {
			assert.FailNow(t, "unreachable")
			return nil
		}).
		BlockLast(context.Background())
	assert.NoError(t, err)
	assert.Nil(t, last)
	first, err := flux.Empty().BlockFirst(context.Background())
	assert.NoError(t, err)
	assert.Nil(t, first)
}

func TestError(t *testing.T) {
	err := errors.New("boom")
	_, _ = flux.Error(err).
		DoOnNext(func(input payload.Payload) error {
			assert.FailNow(t, "unreachable")
			return nil
		}).
		DoOnError(func(e error) {
			assert.Equal(t, err, e)
		}).
		BlockLast(context.Background())
}

func TestClone(t *testing.T) {
	const total = 10
	source := flux.Create(func(ctx context.Context, s flux.Sink) {
		for i := 0; i < total; i++ {
			s.Next(payload.NewString(fmt.Sprintf("data_%d", i), ""))
		}
		s.Complete()
	})
	clone := flux.Clone(source)

	c := atomic.NewInt32(0)
	last, err := clone.
		DoOnNext(func(input payload.Payload) error {
			c.Inc()
			return nil
		}).
		DoOnError(func(e error) {
			assert.FailNow(t, "unreachable")
		}).
		BlockLast(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, fmt.Sprintf("data_%d", total-1), last.DataUTF8())
	assert.Equal(t, int32(total), c.Load())
}

func TestRaw(t *testing.T) {
	const total = 10
	c := atomic.NewInt32(0)
	f := flux.
		Raw(reactorFlux.Range(0, total).Map(func(v reactor.Any) (reactor.Any, error) {
			return payload.NewString(fmt.Sprintf("data_%d", v.(int)), ""), nil
		}))
	last, err := f.
		DoOnNext(func(input payload.Payload) error {
			c.Inc()
			return nil
		}).
		BlockLast(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, int32(total), c.Load())
	assert.Equal(t, fmt.Sprintf("data_%d", total-1), last.DataUTF8())

	c.Store(0)
	const take = 3
	last, err = f.Take(take).
		DoOnNext(func(input payload.Payload) error {
			c.Inc()
			return nil
		}).
		BlockLast(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, "data_2", last.DataUTF8())
	assert.Equal(t, int32(take), c.Load())
}

func TestJust(t *testing.T) {
	c := atomic.NewInt32(0)
	last, err := flux.
		Just(
			payload.NewString("foo", ""),
			payload.NewString("bar", ""),
			payload.NewString("qux", ""),
		).
		DoOnNext(func(input payload.Payload) error {
			c.Inc()
			return nil
		}).
		BlockLast(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, int32(3), c.Load())
	assert.Equal(t, "qux", last.DataUTF8())
}

func TestCreate(t *testing.T) {
	const total = 10
	f := flux.Create(func(i context.Context, sink flux.Sink) {
		for i := 0; i < total; i++ {
			sink.Next(payload.NewString(fmt.Sprintf("foo_%04d", i), fmt.Sprintf("bar_%04d", i)))
		}
		sink.Complete()
	})

	var su rx.Subscription

	done := make(chan struct{})
	nextRequests := atomic.NewInt32(0)

	f.
		DoOnNext(func(input payload.Payload) error {
			fmt.Println("next:", input)
			su.Request(1)
			return nil
		}).
		DoOnRequest(func(n int) {
			fmt.Println("request:", n)
			nextRequests.Add(int32(n))
		}).
		DoFinally(func(s rx.SignalType) {
			fmt.Println("finally")
			close(done)
		}).
		DoOnComplete(func() {
			fmt.Println("complete")
		}).
		Subscribe(context.Background(), rx.OnSubscribe(func(ctx context.Context, s rx.Subscription) {
			su = s
			su.Request(1)
		}))
	<-done
	assert.Equal(t, int32(total+1), nextRequests.Load())
}

func TestMap(t *testing.T) {
	last, err := flux.
		Just(payload.NewString("hello", "")).
		Map(func(p payload.Payload) (payload.Payload, error) {
			return payload.NewString(p.DataUTF8()+" world", ""), nil
		}).
		BlockLast(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, "hello world", last.DataUTF8())
}

func TestProcessor(t *testing.T) {
	processor := flux.CreateProcessor()
	time.AfterFunc(1*time.Second, func() {
		processor.Next(payload.NewString("111", ""))
	})
	time.AfterFunc(2*time.Second, func() {
		processor.Next(payload.NewString("222", ""))
		processor.Complete()
	})

	done := make(chan struct{})

	processor.
		DoOnNext(func(input payload.Payload) error {
			fmt.Println("next:", input)
			return nil
		}).
		DoFinally(func(s rx.SignalType) {
			close(done)
		}).
		Subscribe(context.Background())
	<-done
}

func TestSwitchOnFirst(t *testing.T) {
	flux.Create(func(ctx context.Context, s flux.Sink) {
		s.Next(payload.NewString("5", ""))
		for i := 0; i < 10; i++ {
			s.Next(payload.NewString(fmt.Sprintf("%d", i), ""))
		}
		s.Complete()
	}).SwitchOnFirst(func(s flux.Signal, f flux.Flux) flux.Flux {
		v, ok := s.Value()
		if !ok {
			return f
		}
		first, _ := strconv.Atoi(v.DataUTF8())
		return f.Filter(func(input payload.Payload) bool {
			n, _ := strconv.Atoi(input.DataUTF8())
			return n > first
		})
	}).Subscribe(context.Background(), rx.OnNext(func(input payload.Payload) error {
		fmt.Println("next:", input.DataUTF8())
		return nil
	}))
}

func TestFluxRequest(t *testing.T) {
	f := flux.Create(func(ctx context.Context, s flux.Sink) {
		for i := 0; i < 10; i++ {
			s.Next(payload.NewString(fmt.Sprintf("DD_%04d", i), ""))
		}
		s.Complete()
	})

	var su rx.Subscription

	sub := rx.NewSubscriber(
		rx.OnNext(func(input payload.Payload) error {
			fmt.Println("onNext:", input)
			su.Request(1)
			return nil
		}),
		rx.OnComplete(func() {
			fmt.Println("complete")
		}),
		rx.OnSubscribe(func(ctx context.Context, s rx.Subscription) {
			su = s
			su.Request(1)
			fmt.Println("request:", 1)
		}),
	)

	f.SubscribeWith(context.Background(), sub)

}

func TestProxy_BlockLast(t *testing.T) {
	last, err := flux.Create(func(ctx context.Context, s flux.Sink) {
		for i := 0; i < 10; i++ {
			s.Next(payload.NewString(fmt.Sprintf("DD_%04d", i), ""))
		}
		s.Complete()
	}).BlockLast(context.Background())
	assert.NoError(t, err, "err occurred")
	fmt.Println(last)
}

func TestFluxProcessorWithRequest(t *testing.T) {
	f := flux.Create(func(i context.Context, sink flux.Sink) {
		for i := 0; i < 3; i++ {
			sink.Next(payload.NewString("world", fmt.Sprintf("%d", i)))
		}
		sink.Complete()
	})

	var su rx.Subscription

	sub := rx.NewSubscriber(
		rx.OnNext(func(input payload.Payload) error {
			su.Request(1)
			return nil
		}),
		rx.OnSubscribe(func(ctx context.Context, s rx.Subscription) {
			su = s
			su.Request(1)
		}),
	)

	done := make(chan struct{})

	f.
		DoFinally(func(s rx.SignalType) {
			close(done)
		}).
		SubscribeOn(scheduler.Parallel()).
		SubscribeWith(context.Background(), sub)
	<-done
}

func TestCreateFromChannel(t *testing.T) {
	payloads := make(chan payload.Payload)
	err := make(chan error)

	go func() {
		defer close(payloads)
		defer close(err)

		for i := 1; i <= 10000; i++ {
			p := payload.NewString(strconv.Itoa(i), strconv.Itoa(i))
			payloads <- p
		}
	}()

	background := context.Background()
	last, e := flux.
		CreateFromChannel(payloads, err).
		BlockLast(background)

	if e != nil {
		t.Error(e)
	}

	assert.Equal(t, "10000", last.DataUTF8())

	m, _ := last.MetadataUTF8()
	assert.Equal(t, "10000", m)
}

func TestCreateFromChannelAndEmitError(t *testing.T) {
	payloads := make(chan payload.Payload)
	err := make(chan error)

	go func() {
		defer close(payloads)
		defer close(err)
		err <- errors.New("boom")
	}()

	_, e := flux.CreateFromChannel(payloads, err).BlockLast(context.Background())
	if e == nil {
		t.Fail()
	}
}

func TestCreateFromChannelWithNoEmitsOrErrors(t *testing.T) {
	payloads := make(chan payload.Payload)
	err := make(chan error)

	go func() {
		defer close(payloads)
		defer close(err)
	}()

	background := context.Background()
	_, e := flux.
		CreateFromChannel(payloads, err).
		BlockLast(background)

	if e != nil {
		t.Fail()
	}
}

func TestToChannel(t *testing.T) {
	payloads := make(chan payload.Payload)
	err := make(chan error)

	go func() {
		defer close(payloads)
		defer close(err)

		for i := 1; i <= 10; i++ {
			p := payload.NewString(strconv.Itoa(i), strconv.Itoa(i))
			payloads <- p
		}
	}()

	f := flux.CreateFromChannel(payloads, err)

	valueChan, errChan := f.ToChan(context.Background(), 0)

	var count int
loop:
	for {
		select {
		case _, ok := <-valueChan:
			if !ok {
				break loop
			}
			count++
		case err := <-errChan:
			assert.NoError(t, err)
		}
	}
	assert.Equal(t, 10, count)
}

func TestToChannelEmitError(t *testing.T) {
	payloads := make(chan payload.Payload)
	err := make(chan error)

	go func() {
		defer close(payloads)
		defer close(err)

		for i := 1; i <= 10; i++ {
			err <- errors.New("boom")
		}
	}()

	f := flux.CreateFromChannel(payloads, err)

	valChan, errChan := f.ToChan(context.Background(), 0)

loop:
	for {
		select {
		case _, ok := <-valChan:
			if !ok {
				break loop
			}
			assert.Fail(t, "should be unreachable")
		case err := <-errChan:
			assert.Error(t, err, "should return error")
			break loop
		}
	}

}

func TestFlux_BlockSlice(t *testing.T) {
	const n = 10
	arr, err := genRandomFlux(n).BlockSlice(context.Background())
	assert.NoError(t, err)
	assert.Len(t, arr, n)
}

func TestFlux_SubscribeWithChan(t *testing.T) {
	ch := make(chan payload.Payload)
	err := make(chan error)
	done := make(chan struct{})

	const n = 10
	genRandomFlux(n).
		DoFinally(func(s rx.SignalType) {
			close(done)
		}).
		SubscribeOn(scheduler.Parallel()).
		SubscribeWithChan(context.Background(), ch, err)

	var results []payload.Payload

L:
	for {
		select {
		case v := <-ch:
			results = append(results, v)
		case e := <-err:
			assert.NoError(t, e)
		case <-done:
			break L
		}
	}
	assert.Len(t, results, n)
}

func genRandomFlux(n int) flux.Flux {
	return flux.
		Create(func(ctx context.Context, s flux.Sink) {
			for i := 0; i < n; i++ {
				s.Next(payload.NewString("hello", strconv.Itoa(i)))
			}
			s.Complete()
		})
}
