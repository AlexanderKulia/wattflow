package observability

import "context"

type Envelope[T any] struct {
	Data T
	Ctx  context.Context
}
