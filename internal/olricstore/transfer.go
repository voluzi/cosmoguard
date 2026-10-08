package olricstore

import "github.com/olric-data/olric/pkg/storage"

func (e *Engine) Import([]byte, func(uint64, storage.Entry) error) error {
	return storage.ErrNotImplemented
}
func (e *Engine) TransferIterator() storage.TransferIterator { return &transfer{} }

type transfer struct{}

func (*transfer) Next() bool                   { return false }
func (*transfer) Export() ([]byte, int, error) { return nil, 0, storage.ErrNotImplemented }
func (*transfer) Drop(int) error               { return storage.ErrNotImplemented }
