package pbsourcing

import (
	"reflect"

	"google.golang.org/protobuf/proto"
)

type Register struct {
	registerd map[string]func() proto.Message
}

func NewRegister() Register {
	return Register{
		registerd: make(map[string]func() proto.Message),
	}
}

func (r *Register) Get(a aggregate) proto.Message {
	return r.registerd[aggregateType(a)]()
}

func (r *Register) Register(a aggregate, pbEventMessage proto.Message) {
	t := reflect.TypeOf(pbEventMessage).Elem()
	r.registerd[aggregateType(a)] = func() proto.Message {
		n := reflect.New(t)
		return n.Interface().(proto.Message)
	}
}

func (r *Register) AggregateRegistered(a aggregate) bool {
	_, ok := r.registerd[aggregateType(a)]
	return ok
}

func aggregateType(a aggregate) string {
	return reflect.TypeOf(a).Elem().Name()
}
