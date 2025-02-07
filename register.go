package pbsourcing

import (
	"reflect"
)

type Register struct {
	registerd map[string]func() interface{}
}

func NewRegister() Register {
	return Register{
		registerd: make(map[string]func() interface{}),
	}
}

func (r *Register) Get(a aggregate) interface{} {
	return r.registerd[aggregateType(a)]()
}

func (r *Register) Register(a aggregate, pbEventMessage interface{}) {
	t := reflect.TypeOf(pbEventMessage).Elem()
	r.registerd[aggregateType(a)] = func() interface{} {
		n := reflect.New(t)
		return n.Interface()
	}
}

func (r *Register) AggregateRegistered(a aggregate) bool {
	_, ok := r.registerd[aggregateType(a)]
	return ok
}

func aggregateType(a aggregate) string {
	return reflect.TypeOf(a).Elem().Name()
}
