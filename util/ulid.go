/*
 * Copyright 2018 Marco Helmich
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Code modelled after this execellent implementation:
// https://github.com/oklog/ulid/blob/master/ulid.go
// That implementation seems to be eventually inspired by:
// https://firebase.googleblog.com/2015/02/the-2120-ways-to-ensure-unique_68.html
// and
// https://instagram-engineering.com/sharding-ids-at-instagram-1cf5a71e5a5c

package util

// An ULID is a 16 byte Universally Unique Lexicographically Sortable Identifier
// 	The components are encoded as 16 octets.
// 	Each component is encoded with the MSB first (network byte order).
// 	0                   1                   2                   3
// 	0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
// 	+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
// 	|                      32_bit_uint_time_high                    |
// 	+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
// 	|     16_bit_uint_time_low      |       16_bit_uint_random      |
// 	+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
// 	|                       32_bit_uint_random                      |
// 	+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
// 	|                       32_bit_uint_random                      |
// 	+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//
// A GUID/UUID can be suboptimal for many use-cases because:
//     It isn't the most character efficient way of encoding 128 bits
//     UUID v1/v2 is impractical in many environments, as it requires access to a unique, stable MAC address
//     UUID v3/v5 requires a unique seed and produces randomly distributed IDs, which can cause fragmentation in many data structures
//     UUID v4 provides no other information than randomness which can cause fragmentation in many data structures
//
// A ULID however:
//     Is compatible with UUID/GUID's
//     1.21e+24 unique ULIDs per millisecond (1,208,925,819,614,629,174,706,176 to be exact)
//     Lexicographically sortable
//     Canonically encoded as a 26 character string, as opposed to the 36 character UUID
//     Uses Crockford's base32 for better efficiency and readability (5 bits per character)
//     Case insensitive
//     No special characters (URL safe)

import (
	"bytes"
	"crypto/rand"
	"fmt"

	"github.com/mhelmich/calvin/pb"
)

const (
	// encoding is the base 32 encoding alphabet used in ULID strings
	// see http://www.crockford.com/wrmg/base32.html
	encoding = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
	// encodedSize is the length of a text encoded ULID
	encodedSize = 26
)

// Byte to index table for O(1) lookups when unmarshaling.
// We use 0xFF as sentinel value for invalid indexes.
var dec = [...]byte{
	0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
	0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
	0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
	0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
	0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x01,
	0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0xFF, 0xFF,
	0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E,
	0x0F, 0x10, 0x11, 0xFF, 0x12, 0x13, 0xFF, 0x14, 0x15, 0xFF,
	0x16, 0x17, 0x18, 0x19, 0x1A, 0xFF, 0x1B, 0x1C, 0x1D, 0x1E,
	0x1F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x0A, 0x0B, 0x0C,
	0x0D, 0x0E, 0x0F, 0x10, 0x11, 0xFF, 0x12, 0x13, 0xFF, 0x14,
	0x15, 0xFF, 0x16, 0x17, 0x18, 0x19, 0x1A, 0xFF, 0x1B, 0x1C,
	0x1D, 0x1E, 0x1F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
	0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
	0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
	0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
	0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
	0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
	0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
	0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
	0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
	0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
	0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
	0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
	0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
	0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
}

func NewId() (*ID, error) {
	var id ID
	now := NowUnixUtc()

	id[0] = byte(now >> 40)
	id[1] = byte(now >> 32)
	id[2] = byte(now >> 24)
	id[3] = byte(now >> 16)
	id[4] = byte(now >> 8)
	id[5] = byte(now)

	_, err := rand.Reader.Read(id[6:])
	return &id, err
}

func ParseId(bites []byte) (*ID, error) {
	var id ID
	id128 := &pb.Id128{}
	err := id128.Unmarshal(bites)
	if err != nil {
		return &id, err
	}

	err = Uint64ToBytesInto(id128.Upper, id[:8])
	if err != nil {
		return &id, err
	}

	err = Uint64ToBytesInto(id128.Lower, id[8:])
	if err != nil {
		return &id, err
	}

	return &id, nil
}

func ParseIdFromString(v string) (*ID, error) {
	var id ID
	if len(v) != encodedSize {
		return &id, fmt.Errorf("Value can't be encoded")
	}

	if v[0] > '7' {
		return &id, fmt.Errorf("Buffer overflow")
	}

	// 6 bytes timestamp (48 bits)
	id[0] = ((dec[v[0]] << 5) | dec[v[1]])
	id[1] = ((dec[v[2]] << 3) | (dec[v[3]] >> 2))
	id[2] = ((dec[v[3]] << 6) | (dec[v[4]] << 1) | (dec[v[5]] >> 4))
	id[3] = ((dec[v[5]] << 4) | (dec[v[6]] >> 1))
	id[4] = ((dec[v[6]] << 7) | (dec[v[7]] << 2) | (dec[v[8]] >> 3))
	id[5] = ((dec[v[8]] << 5) | dec[v[9]])

	// 10 bytes of entropy (80 bits)
	id[6] = ((dec[v[10]] << 3) | (dec[v[11]] >> 2))
	id[7] = ((dec[v[11]] << 6) | (dec[v[12]] << 1) | (dec[v[13]] >> 4))
	id[8] = ((dec[v[13]] << 4) | (dec[v[14]] >> 1))
	id[9] = ((dec[v[14]] << 7) | (dec[v[15]] << 2) | (dec[v[16]] >> 3))
	id[10] = ((dec[v[16]] << 5) | dec[v[17]])
	id[11] = ((dec[v[18]] << 3) | dec[v[19]]>>2)
	id[12] = ((dec[v[19]] << 6) | (dec[v[20]] << 1) | (dec[v[21]] >> 4))
	id[13] = ((dec[v[21]] << 4) | (dec[v[22]] >> 1))
	id[14] = ((dec[v[22]] << 7) | (dec[v[23]] << 2) | (dec[v[24]] >> 3))
	id[15] = ((dec[v[24]] << 5) | dec[v[25]])

	return &id, nil
}

func ParseIdFromProto(v *pb.Id128) (*ID, error) {
	var id ID
	err := Uint64ToBytesInto(v.Upper, id[:8])
	if err != nil {
		return &id, err
	}

	err = Uint64ToBytesInto(v.Lower, id[8:])
	if err != nil {
		return &id, err
	}
	return &id, nil
}

// IDs are handles to data structures. The only valid two operations on an id are
// to convert it to a string format (e.g. to save it somewhere) or to compare if with other ids.
type ID [16]byte

// String returns a lexicographically sortable string encoded ULID
// (26 characters, non-standard base 32) e.g. 01CFR5R4JJE9KNRAJFFEAQ2SAJ
// Format: tttttttttteeeeeeeeeeeeeeee where t is time and e is entropy
func (id *ID) String() string {
	bites := make([]byte, encodedSize)

	// 10 byte timestamp
	bites[0] = encoding[(id[0]&224)>>5]
	bites[1] = encoding[id[0]&31]
	bites[2] = encoding[(id[1]&248)>>3]
	bites[3] = encoding[((id[1]&7)<<2)|((id[2]&192)>>6)]
	bites[4] = encoding[(id[2]&62)>>1]
	bites[5] = encoding[((id[2]&1)<<4)|((id[3]&240)>>4)]
	bites[6] = encoding[((id[3]&15)<<1)|((id[4]&128)>>7)]
	bites[7] = encoding[(id[4]&124)>>2]
	bites[8] = encoding[((id[4]&3)<<3)|((id[5]&224)>>5)]
	bites[9] = encoding[id[5]&31]

	// 16 bytes of entropy
	bites[10] = encoding[(id[6]&248)>>3]
	bites[11] = encoding[((id[6]&7)<<2)|((id[7]&192)>>6)]
	bites[12] = encoding[(id[7]&62)>>1]
	bites[13] = encoding[((id[7]&1)<<4)|((id[8]&240)>>4)]
	bites[14] = encoding[((id[8]&15)<<1)|((id[9]&128)>>7)]
	bites[15] = encoding[(id[9]&124)>>2]
	bites[16] = encoding[((id[9]&3)<<3)|((id[10]&224)>>5)]
	bites[17] = encoding[id[10]&31]
	bites[18] = encoding[(id[11]&248)>>3]
	bites[19] = encoding[((id[11]&7)<<2)|((id[12]&192)>>6)]
	bites[20] = encoding[(id[12]&62)>>1]
	bites[21] = encoding[((id[12]&1)<<4)|((id[13]&240)>>4)]
	bites[22] = encoding[((id[13]&15)<<1)|((id[14]&128)>>7)]
	bites[23] = encoding[(id[14]&124)>>2]
	bites[24] = encoding[((id[14]&3)<<3)|((id[15]&224)>>5)]
	bites[25] = encoding[id[15]&31]

	return string(bites)
}

func (id *ID) toProto() *pb.Id128 {
	return &pb.Id128{
		Upper: BytesToUint64(id[:8]),
		Lower: BytesToUint64(id[8:]),
	}
}

func (id *ID) toBytes() ([]byte, error) {
	id128 := &pb.Id128{
		Upper: BytesToUint64(id[:8]),
		Lower: BytesToUint64(id[8:]),
	}

	return id128.Marshal()
}

// CompareTo returns an integer comparing id and other lexicographically.
// The result will be 0 if id==other, -1 if id < other, and +1 if id > other.
func (id *ID) CompareTo(other *ID) int {
	return bytes.Compare(id[:], other[:])
}
