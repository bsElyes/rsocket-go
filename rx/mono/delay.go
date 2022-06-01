package mono

import (
	"time"

	"github.com/bsElyes/rsocket-go/payload"
	"github.com/jjeffcaii/reactor-go"
	"github.com/jjeffcaii/reactor-go/mono"
)

type DelayBuilder time.Duration

func (d DelayBuilder) ToMono(transform func() (payload.Payload, error)) Mono {
	return Raw(mono.Delay(time.Duration(d)).
		Map(func(any reactor.Any) (reactor.Any, error) {
			return transform()
		}))
}

func Delay(delay time.Duration) DelayBuilder {
	return DelayBuilder(delay)
}
