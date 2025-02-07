package main

import (
	"context"

	"github.com/gehhilfe/pbsourcing"
	pb "github.com/gehhilfe/pbsourcing/example/proto"
	"github.com/gehhilfe/pbsourcing/store/memory"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
)

type User struct {
	pbsourcing.AggregateRoot
}

func (u *User) EventType() proto.Message {
	return &pb.FrequentFlierAccountEvent{}
}

func (u *User) Transition(event pbsourcing.Event) {
	userEvent, ok := event.Data.(*pb.FrequentFlierAccountEvent)
	if !ok {
		panic("could not cast event")
	}

	switch userEvent.EventType.(type) {
	case *pb.FrequentFlierAccountEvent_AccountCreated:
		println("Account created")
	}
}

func main() {
	u := &User{}
	r := pbsourcing.NewRegister()
	e := pb.FrequentFlierAccountEvent{}
	r.Register(u, &e)

	created := pb.FrequentFlierAccountEvent{
		EventType: &pb.FrequentFlierAccountEvent_AccountCreated{
			AccountCreated: &pb.FrequentFlierAccountCreated{
				AccountId:         "123",
				OpeningMiles:      0,
				OpeningTierPoints: 0,
			},
		},
	}

	data, err := proto.Marshal(&created)
	if err != nil {
		panic(err)
	}

	instance := r.Get(u)
	err = proto.Unmarshal(data, instance.(proto.Message))

	v := instance.(*pb.FrequentFlierAccountEvent).GetAccountCreated()
	println(v.AccountId)
	println(v.OpeningMiles)
	println(v.OpeningTierPoints)

	created = pb.FrequentFlierAccountEvent{
		EventType: &pb.FrequentFlierAccountEvent_AccountCreated{
			AccountCreated: &pb.FrequentFlierAccountCreated{
				AccountId:         "1234",
				OpeningMiles:      0,
				OpeningTierPoints: 0,
			},
		},
	}

	data, err = proto.Marshal(&created)
	if err != nil {
		panic(err)
	}

	instance = r.Get(u)
	err = proto.Unmarshal(data, instance.(proto.Message))
	v2 := instance.(*pb.FrequentFlierAccountEvent).GetAccountCreated()
	println(v2.AccountId)
	println(v2.OpeningMiles)
	println(v2.OpeningTierPoints)

	sm := memory.NewStore()
	store, _ := sm.Create(pbsourcing.StoreId(uuid.New()), pbsourcing.Metadata{"name": "storeA"})

	repo := pbsourcing.NewEventRepository(r, store)

	nu := &User{}

	nu.TrackChange(nu, &pb.FrequentFlierAccountEvent{
		EventType: &pb.FrequentFlierAccountEvent_AccountCreated{
			AccountCreated: &pb.FrequentFlierAccountCreated{
				AccountId:         "nu",
				OpeningMiles:      10,
				OpeningTierPoints: 20,
			},
		},
	})
	nu.TrackChange(nu, &pb.FrequentFlierAccountEvent{
		EventType: &pb.FrequentFlierAccountEvent_PromotedToGoldStatus{
			PromotedToGoldStatus: &pb.PromotedToGoldStatus{},
		},
	})

	err = repo.Save(nu)
	id := nu.AggregateRoot.ID()
	nu = &User{}
	err = repo.Load(context.Background(), id, nu)
}
