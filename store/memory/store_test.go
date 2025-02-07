package memory_test

import (
	"testing"

	"github.com/gehhilfe/pbsourcing"
	pb "github.com/gehhilfe/pbsourcing/proto"
	"github.com/gehhilfe/pbsourcing/store/memory"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func Test(t *testing.T) {
	sm := memory.NewStore()

	// Should contain no stores.
	for s := range sm.List(pbsourcing.Metadata{}) {
		t.Errorf("unexpected store: %v", s)
	}

	// Should create a new store.
	storeAId := pbsourcing.StoreId(uuid.New())
	storeA, err := sm.Create(storeAId, pbsourcing.Metadata{"name": "storeA"})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	// Should contain one store.
	ctr := 0
	for _ = range sm.List(pbsourcing.Metadata{}) {
		ctr++
	}
	if ctr != 1 {
		t.Errorf("unexpected store count: %v", ctr)
	}

	// Should create a new store.
	storeB := pbsourcing.StoreId(uuid.New())
	_, err = sm.Create(storeB, pbsourcing.Metadata{"name": "storeB"})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	// Should contain one store.
	ctr = 0
	for _ = range sm.List(pbsourcing.Metadata{}) {
		ctr++
	}
	if ctr != 2 {
		t.Errorf("unexpected store count: %v", ctr)
	}
	d, _ := anypb.New(wrapperspb.Bool(true))

	storeA.Append(&pb.SubStoreEvent{
		AggregateId:      &pb.Id{Id: storeAId[:]},
		AggregateType:    "",
		AggregateVersion: 1,
		StoreVersion:     1,
		CreatedAt:        timestamppb.Now(),
		Data:             d,
		Metadata:         pbsourcing.Metadata{"name": "storeA"},
	})
}
