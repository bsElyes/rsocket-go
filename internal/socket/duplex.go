package socket

import (
	"context"
	"runtime"
	"sync"
	"time"

	"github.com/jjeffcaii/reactor-go"
	"github.com/jjeffcaii/reactor-go/scheduler"
	"github.com/pkg/errors"
	"github.com/rsocket/rsocket-go/core"
	"github.com/rsocket/rsocket-go/core/framing"
	"github.com/rsocket/rsocket-go/core/transport"
	"github.com/rsocket/rsocket-go/internal/bytesconv"
	"github.com/rsocket/rsocket-go/internal/common"
	"github.com/rsocket/rsocket-go/internal/fragmentation"
	"github.com/rsocket/rsocket-go/internal/loopywriter"
	"github.com/rsocket/rsocket-go/internal/misc"
	"github.com/rsocket/rsocket-go/internal/queue"
	"github.com/rsocket/rsocket-go/internal/tpfactory"
	"github.com/rsocket/rsocket-go/lease"
	"github.com/rsocket/rsocket-go/logger"
	"github.com/rsocket/rsocket-go/payload"
	"github.com/rsocket/rsocket-go/rx"
	"github.com/rsocket/rsocket-go/rx/flux"
	"github.com/rsocket/rsocket-go/rx/mono"
	"go.uber.org/atomic"
)

const _minRequestSchedulerSize = 1000

var errSocketClosed = errors.New("rsocket: socket closed already")
var errRequestFailed = errors.New("rsocket: send request failed")

var (
	unsupportedRequestStream   = []byte("Request-Stream not implemented.")
	unsupportedRequestResponse = []byte("Request-Response not implemented.")
	unsupportedRequestChannel  = []byte("Request-Channel not implemented.")
)

func mustExecute(sc scheduler.Scheduler, handler func()) {
	if err := sc.Worker().Do(handler); err == nil {
		return
	}
	go handler()
}

// DuplexConnection represents a socket of RSocket which can be a requester or a responder.
type DuplexConnection struct {
	tf             *tpfactory.TransportFactory
	reqSche        scheduler.Scheduler
	resSche        scheduler.Scheduler
	destroyReqSche bool
	locker         sync.RWMutex
	counter        *core.TrafficCounter
	cq             *loopywriter.CtrlQueue
	lw             *loopywriter.LoopyWriter
	responder      Responder
	messages       *map32 // key=streamID, value=callback
	sids           StreamID
	mtu            int
	fragments      *map32 // key=streamID, value=Joiner
	writeDone      chan struct{}
	done           chan struct{}
	keepalive      time.Duration
	e              error
	leases         lease.Factory
	closed         *atomic.Bool
}

// SetError sets error for current socket.
func (dc *DuplexConnection) SetError(err error) {
	dc.locker.Lock()
	dc.e = err
	dc.locker.Unlock()
}

// GetError get the error set.
func (dc *DuplexConnection) GetError() (err error) {
	dc.locker.RLock()
	err = dc.e
	dc.locker.RUnlock()
	return
}

func (dc *DuplexConnection) nextStreamID() (sid uint32) {
	var firstLap bool
	for {
		// There's no required to check StreamID conflicts.
		sid, firstLap = dc.sids.Next()
		if firstLap {
			return
		}
		_, ok := dc.messages.Load(sid)
		if !ok {
			return
		}
	}
}

// Close close current socket.
func (dc *DuplexConnection) Close() error {
	if !dc.closed.CAS(false, true) {
		return nil
	}
	defer func() {
		dc.lw.Dispose(func(frame core.WriteableFrame) {
			frame.Done()
		})
	}()

	close(dc.done)
	tp := dc.tf.Destroy()

	// wait for write loop end
	<-dc.writeDone

	if tp != nil {
		dc.destroyTransport(tp)
	}

	err := dc.GetError()
	if err == nil {
		dc.destroyHandler(errSocketClosed)
	} else {
		dc.destroyHandler(err)
	}

	dc.destroyFragment()

	if dc.destroyReqSche {
		_ = dc.reqSche.Close()
	}

	return err
}

func (dc *DuplexConnection) destroyTransport(tp *transport.Transport) {
	if dc.e == nil {
		dc.locker.Lock()
		defer dc.locker.Unlock()
		dc.e = tp.Close()
	} else {
		_ = tp.Close()
	}
}

func (dc *DuplexConnection) destroyHandler(err error) {
	defer dc.messages.Destroy()
	// TODO: optimize callback map
	var callbacks []callback
	dc.messages.Range(func(sid uint32, v interface{}) bool {
		callbacks = append(callbacks, v.(callback))
		return true
	})
	for _, next := range callbacks {
		next.stopWithError(err)
	}
}

func (dc *DuplexConnection) destroyFragment() {
	dc.fragments.Range(func(u uint32, i interface{}) bool {
		common.TryRelease(i)
		return true
	})
	dc.fragments.Destroy()
}

// FireAndForget start a request of FireAndForget.
func (dc *DuplexConnection) FireAndForget(req payload.Payload) {
	data := req.Data()
	size := core.FrameHeaderLen + len(req.Data())
	m, ok := req.Metadata()
	if ok {
		size += 3 + len(m)
	}
	sid := dc.nextStreamID()

	releasable, isReleasable := req.(common.Releasable)
	if isReleasable {
		releasable.IncRef()
	}

	if !dc.shouldSplit(size) {
		outMsg := framing.NewWriteableFireAndForgetFrame(sid, data, m, 0)
		if isReleasable {
			outMsg.HandleDone(func() {
				releasable.Release()
			})
		}
		dc.sendFrame(outMsg)
		return
	}
	dc.doSplit(data, m, func(index int, result fragmentation.SplitResult) {
		var outMsg core.WriteableFrame
		if index == 0 {
			outMsg = framing.NewWriteableFireAndForgetFrame(sid, result.Data, result.Metadata, result.Flag)
		} else {
			outMsg = framing.NewWriteablePayloadFrame(sid, result.Data, result.Metadata, result.Flag|core.FlagNext)
		}

		if !result.Flag.Check(core.FlagFollow) && isReleasable {
			releasable.Release()
		}
		dc.sendFrame(outMsg)
	})
}

// MetadataPush start a request of MetadataPush.
func (dc *DuplexConnection) MetadataPush(payload payload.Payload) {
	if dc.closed.Load() {
		return
	}
	metadata, _ := payload.Metadata()
	dc.sendFrame(framing.NewWriteableMetadataPushFrame(metadata))
}

// RequestResponse start a request of RequestResponse.
func (dc *DuplexConnection) RequestResponse(req payload.Payload) (res mono.Mono) {
	if dc.closed.Load() {
		res = mono.Error(errSocketClosed)
		return
	}

	sid := dc.nextStreamID()

	handler := &requestResponseCallback{}

	onFinally := func(s reactor.SignalType, d reactor.Disposable) {
		common.TryRelease(handler.cache)
		d.Dispose()
		if s == reactor.SignalTypeCancel {
			dc.sendFrame(framing.NewWriteableCancelFrame(sid))
		}
		dc.unregister(sid)
	}

	m, s, _ := mono.NewProcessor(dc.reqSche, onFinally)
	handler.sink = s

	dc.register(sid, handler)

	res = m

	data := req.Data()
	metadata, _ := req.Metadata()

	// sending...
	size := framing.CalcPayloadFrameSize(data, metadata)

	releasable, isReleasable := req.(common.Releasable)
	if isReleasable {
		releasable.IncRef()
	}

	// mtu disabled
	if !dc.shouldSplit(size) {
		toBeSent := framing.NewWriteableRequestResponseFrame(sid, data, metadata, 0)
		if isReleasable {
			toBeSent.HandleDone(func() {
				releasable.Release()
			})
		}
		if ok := dc.sendFrame(toBeSent); !ok {
			dc.killCallback(sid)
		}
		return
	}

	// mtu enabled
	dc.doSplit(data, metadata, func(index int, result fragmentation.SplitResult) {
		var toBeSent core.WriteableFrame
		if index == 0 {
			toBeSent = framing.NewWriteableRequestResponseFrame(sid, result.Data, result.Metadata, result.Flag)
		} else {
			toBeSent = framing.NewWriteablePayloadFrame(sid, result.Data, result.Metadata, result.Flag|core.FlagNext)
		}

		// Add release hook at last frame.
		if !result.Flag.Check(core.FlagFollow) && isReleasable {
			toBeSent.HandleDone(func() {
				releasable.Release()
			})
		}

		if ok := dc.sendFrame(toBeSent); !ok {
			dc.killCallback(sid)
		}
	})

	return
}

// RequestStream start a request of RequestStream.
func (dc *DuplexConnection) RequestStream(sending payload.Payload) (ret flux.Flux) {
	if dc.closed.Load() {
		ret = flux.Error(errSocketClosed)
		return
	}

	sid := dc.nextStreamID()
	pc := flux.CreateProcessor()

	dc.register(sid, requestStreamCallback{pc: pc})

	requested := atomic.NewBool(false)

	// Create a queue to save those payloads to be released.
	toBeReleased := queue.NewLKQueue()

	ret = pc.
		DoFinally(func(sig rx.SignalType) {
			if sig == rx.SignalCancel {
				dc.sendFrame(framing.NewWriteableCancelFrame(sid))
			}
			dc.unregister(sid)
			for {
				next := toBeReleased.Dequeue()
				if next == nil {
					break
				}
				next.(common.Releasable).Release()
			}
		}).
		DoOnNext(func(input payload.Payload) error {
			if nextRelease := toBeReleased.Dequeue(); nextRelease != nil {
				nextRelease.(common.Releasable).Release()
			}
			if _, ok := input.(common.Releasable); ok {
				toBeReleased.Enqueue(input)
			}
			return nil
		}).
		DoOnRequest(func(n int) {
			n32 := ToUint32RequestN(n)

			// Send RequestN at first time.
			if !requested.CAS(false, true) {
				done := make(chan struct{})
				frameN := framing.NewWriteableRequestNFrame(sid, n32, 0)
				frameN.HandleDone(func() {
					close(done)
				})
				if dc.sendFrame(frameN) {
					<-done
				}
				return
			}

			releasable, isReleasable := sending.(common.Releasable)

			if isReleasable {
				releasable.IncRef()
			}

			data := sending.Data()
			metadata, _ := sending.Metadata()

			size := framing.CalcPayloadFrameSize(data, metadata) + 4
			if !dc.shouldSplit(size) {
				toBeSent := framing.NewWriteableRequestStreamFrame(sid, n32, data, metadata, 0)

				if isReleasable {
					toBeSent.HandleDone(func() {
						releasable.Release()
					})
				}

				if ok := dc.sendFrame(toBeSent); !ok {
					dc.killCallback(sid)
				}
				return
			}

			dc.doSplitSkip(4, data, metadata, func(index int, result fragmentation.SplitResult) {
				var toBeSent core.WriteableFrame
				if index == 0 {
					toBeSent = framing.NewWriteableRequestStreamFrame(sid, n32, result.Data, result.Metadata, result.Flag)
				} else {
					toBeSent = framing.NewWriteablePayloadFrame(sid, result.Data, result.Metadata, result.Flag|core.FlagNext)
				}

				// Add release hook at last frame.
				if !result.Flag.Check(core.FlagFollow) && isReleasable {
					toBeSent.HandleDone(func() {
						releasable.Release()
					})
				}

				if ok := dc.sendFrame(toBeSent); !ok {
					dc.killCallback(sid)
				}
			})
		})
	return
}

func (dc *DuplexConnection) killCallback(sid uint32) {
	cb, ok := dc.messages.Load(sid)
	if !ok {
		return
	}
	cb.(callback).stopWithError(errRequestFailed)
}

// RequestChannel start a request of RequestChannel.
func (dc *DuplexConnection) RequestChannel(sending flux.Flux) (ret flux.Flux) {
	if dc.closed.Load() {
		ret = flux.Error(errSocketClosed)
		return
	}

	sid := dc.nextStreamID()

	receiving := flux.CreateProcessor()

	rcvRequested := atomic.NewBool(false)

	toBeReleased := queue.NewLKQueue()

	sendResult := make(chan error)

	ret = receiving.
		DoFinally(func(sig rx.SignalType) {
			dc.unregister(sid)
			// release resources.
			for {
				next := toBeReleased.Dequeue()
				if next == nil {
					break
				}
				next.(common.Releasable).Release()
			}
			// process sending result
			e, ok := <-sendResult
			if ok {
				dc.writeError(sid, e)
			} else {
				complete := framing.NewWriteablePayloadFrame(sid, nil, nil, core.FlagComplete)
				done := make(chan struct{})
				complete.HandleDone(func() {
					close(done)
				})
				if dc.sendFrame(complete) {
					<-done
				}
			}
		}).
		DoOnNext(func(next payload.Payload) error {
			if nextRelease := toBeReleased.Dequeue(); nextRelease != nil {
				nextRelease.(common.Releasable).Release()
			}
			if _, ok := next.(common.Releasable); ok {
				toBeReleased.Enqueue(next)
			}
			return nil
		}).
		DoOnRequest(func(initN int) {
			n := ToUint32RequestN(initN)
			if !rcvRequested.CAS(false, true) {
				frameN := framing.NewWriteableRequestNFrame(sid, n, 0)
				done := make(chan struct{})
				frameN.HandleDone(func() {
					close(done)
				})
				if dc.sendFrame(frameN) {
					<-done
				}
				return
			}

			sub := requestChannelSubscriber{
				sid:          sid,
				n:            n,
				dc:           dc,
				sndRequested: atomic.NewBool(false),
				rcv:          receiving,
				result:       sendResult,
			}
			sending.SubscribeOn(dc.reqSche).SubscribeWith(context.Background(), sub)
		})
	return ret
}

func (dc *DuplexConnection) onFrameRequestResponse(frame core.BufferedFrame) error {
	// fragment
	receiving, ok := dc.doFragment(frame.(*framing.RequestResponseFrame))
	if !ok {
		return nil
	}
	return dc.respondRequestResponse(receiving)
}

func (dc *DuplexConnection) respondRequestResponse(receiving fragmentation.HeaderAndPayload) error {
	sid := receiving.Header().StreamID()

	// execute socket handler
	sending, err := func() (resp mono.Mono, err error) {
		defer func() {
			rec := recover()
			if rec == nil {
				return
			}
			if e, ok := rec.(error); ok {
				err = errors.WithStack(e)
			} else {
				err = errors.Errorf("%v", e)
			}
			logger.Errorf("handle request-response failed: %+v\n", err)
		}()
		resp = dc.responder.RequestResponse(receiving)
		if resp == nil {
			err = framing.NewWriteableErrorFrame(sid, core.ErrorCodeApplicationError, unsupportedRequestResponse)
		}
		return
	}()
	// sending error with panic
	if err != nil {
		common.TryRelease(receiving)
		dc.writeError(sid, err)
		return nil
	}

	// async subscribe publisher
	sub := borrowRequestResponseSubscriber(dc, sid, receiving)
	if mono.IsSubscribeAsync(sending) {
		sending.SubscribeWith(context.Background(), sub)
		return nil
	}
	mustExecute(dc.resSche, func() {
		sending.SubscribeWith(context.Background(), sub)
	})
	return nil
}

func (dc *DuplexConnection) onFrameRequestChannel(input core.BufferedFrame) error {
	receiving, ok := dc.doFragment(input.(*framing.RequestChannelFrame))
	if !ok {
		return nil
	}
	return dc.respondRequestChannel(receiving)
}

func (dc *DuplexConnection) respondRequestChannel(req fragmentation.HeaderAndPayload) error {
	// seek initRequestN
	initRequestN := extractRequestStreamInitN(req)

	sid := req.Header().StreamID()
	receivingProcessor := flux.CreateProcessor()

	finallyRequests := atomic.NewInt32(0)

	toBeReleased := queue.NewLKQueue()

	receiving := receivingProcessor.
		DoFinally(func(sig rx.SignalType) {
			if finallyRequests.Inc() == 2 {
				dc.unregister(sid)
			}
			if sig == rx.SignalCancel {
				dc.sendFrame(framing.NewWriteableCancelFrame(sid))
			}
			for {
				next := toBeReleased.Dequeue()
				if next == nil {
					break
				}
				next.(common.Releasable).Release()
			}
		}).
		DoOnNext(func(input payload.Payload) error {
			if nextRelease := toBeReleased.Dequeue(); nextRelease != nil {
				nextRelease.(common.Releasable).Release()
			}
			if _, ok := input.(common.Releasable); ok {
				toBeReleased.Enqueue(input)
			}
			return nil
		}).
		DoOnRequest(func(n int) {
			frameN := framing.NewWriteableRequestNFrame(sid, ToUint32RequestN(n), 0)
			done := make(chan struct{})
			frameN.HandleDone(func() {
				close(done)
			})
			if dc.sendFrame(frameN) {
				<-done
			}
		}).
		SubscribeOn(dc.reqSche)

	// TODO: if receiving == sending ???
	sending, err := func() (resp flux.Flux, err error) {
		defer func() {
			rec := recover()
			if rec == nil {
				return
			}
			if e, ok := rec.(error); ok {
				err = errors.WithStack(e)
			} else {
				err = errors.Errorf("%v", e)
			}
			logger.Errorf("handle request-channel failed: %+v\n", err)
		}()
		resp = dc.responder.RequestChannel(receiving)
		if resp == nil {
			err = framing.NewWriteableErrorFrame(sid, core.ErrorCodeApplicationError, unsupportedRequestChannel)
		}
		return
	}()

	if err != nil {
		common.TryRelease(receiving)
		dc.writeError(sid, err)
		return nil
	}

	receivingProcessor.Next(req)

	// Ensure registering message success before func end.
	subscribed := make(chan struct{})

	// Create subscriber
	sub := respondChannelSubscriber{
		sid:        sid,
		n:          initRequestN,
		dc:         dc,
		rcv:        receivingProcessor,
		subscribed: subscribed,
		calls:      finallyRequests,
	}

	mustExecute(dc.reqSche, func() {
		sending.SubscribeWith(context.Background(), sub)
	})

	<-subscribed

	return nil
}

func (dc *DuplexConnection) respondMetadataPush(input core.BufferedFrame) error {
	req := input.(*framing.MetadataPushFrame)
	mustExecute(dc.resSche, func() {
		defer func() {
			req.Release()
			rec := recover()
			if rec == nil {
				return
			}
			var err error
			if e, ok := rec.(error); ok {
				err = errors.WithStack(e)
			} else {
				err = errors.Errorf("%v", e)
			}
			logger.Errorf("handle metadata-push failed: %+v\n", err)
		}()
		dc.responder.MetadataPush(req)
	})
	return nil
}

func (dc *DuplexConnection) onFrameFNF(frame core.BufferedFrame) error {
	receiving, ok := dc.doFragment(frame.(*framing.FireAndForgetFrame))
	if !ok {
		return nil
	}
	return dc.respondFireAndForget(receiving)
}

func (dc *DuplexConnection) respondFireAndForget(receiving fragmentation.HeaderAndPayload) error {
	mustExecute(dc.resSche, func() {
		defer func() {
			common.TryRelease(receiving)
			rec := recover()
			if rec == nil {
				return
			}
			var err error
			if e, ok := rec.(error); ok {
				err = errors.WithStack(e)
			} else {
				err = errors.Errorf("%v", e)
			}
			logger.Errorf("handle fire-and-forget failed: %+v\n", err)
		}()
		dc.responder.FireAndForget(receiving)
	})
	return nil
}

func (dc *DuplexConnection) onFrameRequestStream(frame core.BufferedFrame) error {
	receiving, ok := dc.doFragment(frame.(*framing.RequestStreamFrame))
	if !ok {
		return nil

	}
	return dc.respondRequestStream(receiving)
}

func (dc *DuplexConnection) respondRequestStream(receiving fragmentation.HeaderAndPayload) error {
	sid := receiving.Header().StreamID()
	n := extractRequestStreamInitN(receiving)

	// execute request stream handler
	sending, err := func() (resp flux.Flux, err error) {
		defer func() {
			rec := recover()
			if rec == nil {
				return
			}
			if e, ok := rec.(error); ok {
				err = errors.WithStack(e)
			} else {
				err = errors.Errorf("%v", err)
			}
			logger.Errorf("handle request-stream failed: %+v\n", err)
		}()
		resp = dc.responder.RequestStream(receiving)
		if resp == nil {
			err = framing.NewWriteableErrorFrame(sid, core.ErrorCodeApplicationError, unsupportedRequestStream)
		}
		return
	}()

	// send error with panic
	if err != nil {
		common.TryRelease(receiving)
		dc.writeError(sid, err)
		return nil
	}

	// async subscribe publisher
	sub := borrowRequestStreamSubscriber(receiving, dc, sid, n)
	sending.SubscribeOn(dc.resSche).SubscribeWith(context.Background(), sub)
	return nil
}

func (dc *DuplexConnection) writeError(sid uint32, e error) {
	// ignore sending error because current socket has been closed.
	if IsSocketClosedError(e) {
		return
	}
	switch err := e.(type) {
	case *framing.WriteableErrorFrame:
		dc.sendFrame(err)
	case core.CustomError:
		dc.sendFrame(framing.NewWriteableErrorFrame(sid, err.ErrorCode(), err.ErrorData()))
	default:
		errFrame := framing.NewWriteableErrorFrame(
			sid,
			core.ErrorCodeApplicationError,
			bytesconv.StringToBytes(e.Error()),
		)
		dc.sendFrame(errFrame)
	}
}

// SetResponder sets a responder for current socket.
func (dc *DuplexConnection) SetResponder(responder Responder) {
	dc.responder = responder
}

func (dc *DuplexConnection) onFrameKeepalive(frame core.BufferedFrame) (err error) {
	defer frame.Release()
	f := frame.(*framing.KeepaliveFrame)
	if !f.HasFlag(core.FlagRespond) {
		return
	}
	// TODO: optimize, if keepalive frame support modify data.
	data := common.CloneBytes(f.Data())
	k := framing.NewWriteableKeepaliveFrame(f.LastReceivedPosition(), data, false)
	dc.sendFrame(k)
	return
}

func (dc *DuplexConnection) deleteFragment(sid uint32) {
	v, ok := dc.fragments.Load(sid)
	if !ok {
		return
	}
	dc.fragments.Delete(sid)
	common.TryRelease(v)
}

func (dc *DuplexConnection) onFrameCancel(frame core.BufferedFrame) (err error) {
	sid := frame.Header().StreamID()
	frame.Release()

	defer dc.deleteFragment(sid)

	v, ok := dc.messages.Load(sid)
	if !ok {
		logger.Warnf("unmatched frame CANCEL(id=%d), maybe original request has been cancelled\n", sid)
		return
	}

	switch vv := v.(type) {
	case requestResponseCallbackReverse:
		vv.su.Cancel()
	case requestStreamCallbackReverse:
		vv.su.Cancel()
	default:
		panic("cannot cancel")
	}

	return
}

func (dc *DuplexConnection) onFrameError(input core.BufferedFrame) error {
	defer input.Release()
	f := input.(*framing.ErrorFrame)
	sid := f.Header().StreamID()

	// TODO: avoid clone error
	err := f.ToError()

	v, ok := dc.messages.Load(sid)
	if !ok {
		dc.deleteFragment(sid)
		logger.Warnf("unmatched frame ERROR(id=%d), maybe original request has been cancelled\n", sid)
		return nil
	}

	switch vv := v.(type) {
	case *requestResponseCallback:
		vv.sink.Error(err)
	case requestStreamCallback:
		vv.pc.Error(err)
	case requestChannelCallback:
		vv.rcv.Error(err)
	default:
		return errors.Errorf("illegal value for error: %v", vv)
	}
	return nil
}

func (dc *DuplexConnection) onFrameRequestN(input core.BufferedFrame) error {
	defer input.Release()
	f := input.(*framing.RequestNFrame)
	sid := f.Header().StreamID()
	v, ok := dc.messages.Load(sid)
	if !ok {
		dc.deleteFragment(sid)
		logger.Warnf("unmatched frame REQUEST_N(id=%d), maybe original request has been cancelled\n", sid)
		return nil
	}
	n := ToIntRequestN(f.N())
	switch vv := v.(type) {
	case requestStreamCallbackReverse:
		vv.su.Request(n)
	case requestChannelCallback:
		vv.snd.Request(n)
	case respondChannelCallback:
		vv.snd.Request(n)
	}
	return nil
}

func (dc *DuplexConnection) doFragment(input fragmentation.HeaderAndPayload) (out fragmentation.HeaderAndPayload, ok bool) {
	h := input.Header()
	sid := h.StreamID()
	v, exist := dc.fragments.Load(sid)
	if exist {
		joiner := v.(fragmentation.Joiner)
		ok = joiner.Push(input)
		if ok {
			dc.fragments.Delete(sid)
			out = joiner
		}
		return
	}
	ok = !h.Flag().Check(core.FlagFollow)
	if ok {
		out = input
		return
	}
	dc.fragments.Store(sid, fragmentation.NewJoiner(input))
	return
}

func (dc *DuplexConnection) onFramePayload(frame core.BufferedFrame) error {
	next, ok := dc.doFragment(frame.(*framing.PayloadFrame))
	if !ok {
		return nil
	}
	h := next.Header()

	switch h.Type() {
	case core.FrameTypeRequestFNF:
		return dc.respondFireAndForget(next)
	case core.FrameTypeRequestResponse:
		return dc.respondRequestResponse(next)
	case core.FrameTypeRequestStream:
		return dc.respondRequestStream(next)
	case core.FrameTypeRequestChannel:
		return dc.respondRequestChannel(next)
	}

	sid := h.StreamID()
	v, ok := dc.messages.Load(sid)
	if !ok {
		common.TryRelease(next)
		logger.Warnf("unmatched frame PAYLOAD(id=%d), maybe original request has been cancelled\n", sid)
		return nil
	}

	switch handler := v.(type) {
	case *requestResponseCallback:
		handler.cache = next
		handler.sink.Success(next)
	case requestStreamCallback:
		fg := h.Flag()
		isNext := fg.Check(core.FlagNext)
		if isNext {
			handler.pc.Next(next)
		}
		if fg.Check(core.FlagComplete) {
			if !isNext {
				common.TryRelease(next)
			}
			// Release pure complete payload
			handler.pc.Complete()
		}
	case requestChannelCallback:
		fg := h.Flag()
		isNext := fg.Check(core.FlagNext)
		if isNext {
			handler.rcv.Next(next)
		}
		if fg.Check(core.FlagComplete) {
			if !isNext {
				common.TryRelease(next)
			}
			handler.rcv.Complete()
		}
	case respondChannelCallback:
		fg := h.Flag()
		isNext := fg.Check(core.FlagNext)
		if isNext {
			handler.rcv.Next(next)
		}
		if fg.Check(core.FlagComplete) {
			if !isNext {
				common.TryRelease(next)
			}
			handler.rcv.Complete()
		}
	}
	return nil
}

func (dc *DuplexConnection) clearTransport() {
	dc.tf.Reset()
}

// SetTransport sets a transport for current socket.
func (dc *DuplexConnection) SetTransport(tp *transport.Transport) error {
	tp.Handle(transport.OnCancel, dc.onFrameCancel)
	tp.Handle(transport.OnError, dc.onFrameError)
	tp.Handle(transport.OnRequestN, dc.onFrameRequestN)
	tp.Handle(transport.OnPayload, dc.onFramePayload)
	tp.Handle(transport.OnKeepalive, dc.onFrameKeepalive)
	if dc.responder != nil {
		tp.Handle(transport.OnRequestResponse, dc.onFrameRequestResponse)
		tp.Handle(transport.OnMetadataPush, dc.respondMetadataPush)
		tp.Handle(transport.OnFireAndForget, dc.onFrameFNF)
		tp.Handle(transport.OnRequestStream, dc.onFrameRequestStream)
		tp.Handle(transport.OnRequestChannel, dc.onFrameRequestChannel)
	}
	return dc.tf.Set(tp)
}

func (dc *DuplexConnection) sendFrame(next core.WriteableFrame) (ok bool) {
	if dc.closed.Load() {
		next.Done()
		return
	}
	ok = dc.cq.Enqueue(next)
	if !ok {
		next.Done()
	}
	return
}

func (dc *DuplexConnection) sendPayload(
	sid uint32,
	sending payload.Payload,
	frameFlag core.FrameFlag,
) {

	d := sending.Data()
	m, _ := sending.Metadata()
	size := framing.CalcPayloadFrameSize(d, m)

	releasable, isReleasable := sending.(common.Releasable)
	if isReleasable {
		releasable.IncRef()
	}

	if !dc.shouldSplit(size) {
		toBeSent := framing.NewWriteablePayloadFrame(sid, d, m, frameFlag)
		if isReleasable {
			toBeSent.HandleDone(func() {
				releasable.Release()
			})
		}
		dc.sendFrame(toBeSent)
		return
	}
	dc.doSplit(d, m, func(index int, result fragmentation.SplitResult) {
		flag := result.Flag
		if index == 0 {
			flag |= frameFlag
		} else {
			flag |= core.FlagNext
		}

		// lazy release at last frame
		next := framing.NewWriteablePayloadFrame(sid, result.Data, result.Metadata, flag)

		if !result.Flag.Check(core.FlagFollow) {
			next.HandleDone(func() {
				releasable.Release()
			})
		}
		dc.sendFrame(next)
	})
}

// LoopWrite start write loop
func (dc *DuplexConnection) LoopWrite(ctx context.Context) error {
	defer func() {
		dc.writeDone <- struct{}{}
	}()
	if dc.leases != nil {
		leaseCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		if c, ok := dc.leases.Next(leaseCtx); ok {
			dc.cq.BindLease(c)
		}
	}
	return dc.lw.Run(ctx, dc.keepalive, dc.tf)
}

func (dc *DuplexConnection) doSplit(data, metadata []byte, handler fragmentation.HandleSplitResult) {
	fragmentation.Split(dc.mtu, data, metadata, handler)
}

func (dc *DuplexConnection) doSplitSkip(skip int, data, metadata []byte, handler fragmentation.HandleSplitResult) {
	fragmentation.SplitSkip(dc.mtu, skip, data, metadata, handler)
}

func (dc *DuplexConnection) shouldSplit(size int) bool {
	return size > dc.mtu
}

func (dc *DuplexConnection) register(sid uint32, msg interface{}) {
	dc.messages.Store(sid, msg)
}

func (dc *DuplexConnection) unregister(sid uint32) {
	dc.messages.Delete(sid)
	dc.deleteFragment(sid)
}

// IsSocketClosedError returns true if input error is for socket closed.
func IsSocketClosedError(err error) bool {
	return err == errSocketClosed
}

// NewServerDuplexConnection creates a new server-side DuplexConnection.
func NewServerDuplexConnection(reqSche, resSche scheduler.Scheduler, mtu int, leases lease.Factory) *DuplexConnection {
	return newDuplexConnection(reqSche, resSche, mtu, 0, &serverStreamIDs{}, leases)
}

// NewClientDuplexConnection creates a new client-side DuplexConnection.
func NewClientDuplexConnection(reqSche, resSche scheduler.Scheduler, mtu int, keepaliveInterval time.Duration) *DuplexConnection {
	return newDuplexConnection(reqSche, resSche, mtu, keepaliveInterval, &clientStreamIDs{}, nil)
}

func newDuplexConnection(reqSche, resSche scheduler.Scheduler, mtu int, keepalive time.Duration, sids StreamID, leases lease.Factory) *DuplexConnection {
	destroyReqSche := reqSche == nil
	if destroyReqSche {
		reqSche = scheduler.NewElastic(misc.MaxInt(runtime.NumCPU()<<8, _minRequestSchedulerSize))
	}
	if resSche == nil {
		resSche = scheduler.Elastic()
	}
	c := &DuplexConnection{
		reqSche:        reqSche,
		resSche:        resSche,
		destroyReqSche: destroyReqSche,
		leases:         leases,
		mtu:            mtu,
		messages:       newMap32(),
		sids:           sids,
		fragments:      newMap32(),
		done:           make(chan struct{}),
		writeDone:      make(chan struct{}),
		counter:        core.NewTrafficCounter(),
		keepalive:      keepalive,
		closed:         atomic.NewBool(false),
		tf:             tpfactory.NewTransportFactory(),
	}

	c.cq = loopywriter.NewCtrlQueue(c.done)
	c.lw = loopywriter.NewLoopyWriter(c.cq, true, c.counter)
	return c
}
