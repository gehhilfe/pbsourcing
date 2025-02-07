package pbsourcing

import "github.com/google/uuid"

type StoreId uuid.UUID

func (id StoreId) MarshalText() ([]byte, error) {
	return uuid.UUID(id).MarshalText()
}

func (id *StoreId) UnmarshalText(data []byte) error {
	var u uuid.UUID
	if err := u.UnmarshalText(data); err != nil {
		return err
	}
	*id = StoreId(u)
	return nil
}

func (id *StoreId) Scan(src any) error {
	var u uuid.UUID
	if err := u.Scan(src); err != nil {
		return err
	}
	*id = StoreId(u)
	return nil
}

func (id StoreId) String() string {
	return uuid.UUID(id).String()
}
