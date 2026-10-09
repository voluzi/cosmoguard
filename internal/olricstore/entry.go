// Copyright 2018-2025 The Olric Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package olricstore

import (
	"encoding/binary"

	"github.com/voluzi/olric/pkg/storage"
)

// In-memory layout for an entry:
//
// Key length (uint8), key, TTL, timestamp, last access (int64), value length (uint32), value.

// Entry represents a value with its metadata.
type Entry struct {
	key        string
	ttl        int64
	timestamp  int64
	lastAccess int64
	value      []byte
}

var _ storage.Entry = (*Entry)(nil)

func NewEntry() *Entry {
	return &Entry{}
}

func (e *Entry) SetKey(key string) {
	e.key = key
}

func (e *Entry) Key() string {
	return e.key
}

func (e *Entry) SetValue(value []byte) {
	e.value = value
}

func (e *Entry) Value() []byte {
	return e.value
}

func (e *Entry) SetTTL(ttl int64) {
	e.ttl = ttl
}

func (e *Entry) TTL() int64 {
	return e.ttl
}

func (e *Entry) SetTimestamp(timestamp int64) {
	e.timestamp = timestamp
}

func (e *Entry) Timestamp() int64 {
	return e.timestamp
}

func (e *Entry) SetLastAccess(lastAccess int64) {
	e.lastAccess = lastAccess
}

func (e *Entry) LastAccess() int64 {
	return e.lastAccess
}

func (e *Entry) Encode() []byte {
	var offset int

	klen := uint8(len(e.Key()))
	vlen := len(e.Value())
	length := 29 + len(e.Key()) + vlen

	buf := make([]byte, length)

	// Set key length. It's 1 byte.
	copy(buf[offset:], []byte{klen})
	offset++

	// Set the key.
	copy(buf[offset:], e.Key())
	offset += len(e.Key())

	// Set the TTL. It's 8 bytes.
	binary.BigEndian.PutUint64(buf[offset:], uint64(e.TTL()))
	offset += 8

	// Set the Timestamp. It's 8 bytes.
	binary.BigEndian.PutUint64(buf[offset:], uint64(e.Timestamp()))
	offset += 8

	// Set the LastAccess. It's 8 bytes.
	binary.BigEndian.PutUint64(buf[offset:], uint64(e.LastAccess()))
	offset += 8

	// Set the value length. It's 4 bytes.
	binary.BigEndian.PutUint32(buf[offset:], uint32(len(e.Value())))
	offset += 4

	// Set the value.
	copy(buf[offset:], e.Value())
	return buf
}

func (e *Entry) Decode(buf []byte) {
	*e = Entry{}
	if !validRaw(buf) {
		return
	}
	var offset int

	keyLength := int(buf[offset])
	offset++

	e.key = string(buf[offset : offset+keyLength])
	offset += keyLength

	e.ttl = int64(binary.BigEndian.Uint64(buf[offset : offset+8]))
	offset += 8

	e.timestamp = int64(binary.BigEndian.Uint64(buf[offset : offset+8]))
	offset += 8

	e.lastAccess = int64(binary.BigEndian.Uint64(buf[offset : offset+8]))
	offset += 8

	vlen := binary.BigEndian.Uint32(buf[offset : offset+4])
	offset += 4
	e.value = buf[offset : offset+int(vlen)]
}

// MaxEntryBytes is the native table envelope; equality cannot fit a table.
const MaxEntryBytes = 1 << 20

func validRaw(b []byte) bool {
	if len(b) < 29 || len(b) >= MaxEntryBytes {
		return false
	}
	k := int(b[0])
	return len(b) >= 29+k && uint64(binary.BigEndian.Uint32(b[25+k:29+k])) == uint64(len(b)-29-k)
}
