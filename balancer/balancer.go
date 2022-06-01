// Package balancer defines APIs for load balancing in RSocket.
package balancer

import (
	"context"
	"io"

	"github.com/bsElyes/rsocket-go"
)

// Balancer manage input RSocket clients.
type Balancer interface {
	io.Closer
	// Put puts a new client.
	Put(client rsocket.Client) error
	// PutLabel puts a new client with a label.
	PutLabel(label string, client rsocket.Client) error
	// Next returns next balanced RSocket client.
	Next(context.Context) (rsocket.Client, bool)
	// OnLeave handle events when a client exit.
	OnLeave(fn func(label string))
	//Returns the balancer length
	Len() int
}
