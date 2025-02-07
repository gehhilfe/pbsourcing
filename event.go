package pbsourcing

import (
	"encoding/json"
	"time"

	pb "github.com/gehhilfe/pbsourcing/proto"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Event struct {
	AggregateId   string
	AggregateType string
	Version       uint64
	GlobalVersion uint64
	CreatedAt     time.Time
	Data          proto.Message
	Metadata      map[string]interface{}
}

func (e *Event) ToPb() (*pb.Event, error) {
	data, err := proto.Marshal(e.Data)
	if err != nil {
		return nil, err
	}

	eMetadata, err := json.Marshal(e.Metadata)
	if err != nil {
		return nil, err
	}

	return &pb.Event{
		AggregateId:   e.AggregateId,
		AggregateType: e.AggregateType,
		Version:       e.Version,
		GlobalVersion: e.GlobalVersion,
		CreatedAt:     timestamppb.New(e.CreatedAt),
		Data:          data,
		Metadata:      string(eMetadata),
	}, nil
}
