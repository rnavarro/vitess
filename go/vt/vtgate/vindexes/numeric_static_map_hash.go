/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package vindexes

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/youtube/vitess/go/sqltypes"
)

var (
	_ Functional = (*NumericStaticMapHash)(nil)
)

// NumericStaticMapHash is similar to vindex Numeric but first attempts a lookup via
// a JSON file.
type NumericStaticMapHash struct {
	name   string
	lookup NumericLookupTable
}

func init() {
	Register("numeric_static_map_hash", NewNumericStaticMapHash)
}

// NewNumericStaticMapHash creates a NumericStaticMapHash vindex.
func NewNumericStaticMapHash(name string, params map[string]string) (Vindex, error) {
	jsonPath, ok := params["json_path"]
	if !ok {
		return nil, errors.New("NumericStaticMapHash: Could not find `json_path` param in vschema")
	}

	lt, err := loadNumericLookupTable(jsonPath)
	if err != nil {
		return nil, err
	}

	return &NumericStaticMapHash{
		name:   name,
		lookup: lt,
	}, nil
}

// String returns the name of the vindex.
func (vind *NumericStaticMapHash) String() string {
	return vind.name
}

// Cost returns the cost of this vindex as 1.
func (*NumericStaticMapHash) Cost() int {
	return 1
}

// Verify returns true if ids and ksids match.
func (vind *NumericStaticMapHash) Verify(_ VCursor, ids []sqltypes.Value, ksids [][]byte) ([]bool, error) {
	out := make([]bool, len(ids))
	for i := range ids {
		var keybytes [8]byte
		num, err := sqltypes.ToUint64(ids[i])
		if err != nil {
			return nil, fmt.Errorf("NumericStaticMapHash.Verify: %v", err)
		}
		lookupNum, ok := vind.lookup[num]
		if ok {
			num = lookupNum
		}
		binary.BigEndian.PutUint64(keybytes[:], num)
		out[i] = (bytes.Compare(keybytes[:], ksids[i]) == 0)
	}
	return out, nil
}

// Map returns the associated keyspace ids for the given ids.
func (vind *NumericStaticMapHash) Map(_ VCursor, ids []sqltypes.Value) ([][]byte, error) {
	out := make([][]byte, 0, len(ids))
	for _, id := range ids {
		num, err := sqltypes.ToUint64(id)
		if err != nil {
			return nil, fmt.Errorf("NumericStaticMapHash.Map: %v", err)
		}
		lookupNum, found := vind.lookup[num]
		if found {
			num = lookupNum

			var keybytes [8]byte
			binary.BigEndian.PutUint64(keybytes[:], num)
			out = append(out, keybytes[:])
		} else {
			out = append(out, vhash(num))
		}

	}
	return out, nil
}
