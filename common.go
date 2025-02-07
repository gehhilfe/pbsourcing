package pbsourcing

import "google.golang.org/protobuf/proto"

func CloneOf[M proto.Message](m M) M {
	return proto.Clone(m).(M)
}
