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
	"reflect"
	"testing"

	"strings"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/testfiles"
)

// createNumericStaticMapHashVindex creates the "numeric_static_map" vindex object which is used by
// each test.
//
// IMPORTANT: This code is called per test and must not be called from init()
// because our internal implementation of testfiles.Locate() does not support to
// be called from init().
func createNumericStaticMapHashVindex() (Vindex, error) {
	m := make(map[string]string)
	m["json_path"] = testfiles.Locate("vtgate/numeric_static_map_test.json")
	return CreateVindex("numeric_static_map_hash", "NumericStaticMapHash", m)
}

func TestNumericStaticMapHashCost(t *testing.T) {
	NumericStaticMapHash, err := createNumericStaticMapHashVindex()
	if err != nil {
		t.Fatalf("failed to create vindex: %v", err)
	}
	if NumericStaticMapHash.Cost() != 1 {
		t.Errorf("NumericStaticMapHash.Cost(): %d, want 1", NumericStaticMapHash.Cost())
	}
}

func TestNumericStaticMapHashString(t *testing.T) {
	NumericStaticMapHash, err := createNumericStaticMapHashVindex()
	if err != nil {
		t.Fatalf("failed to create vindex: %v", err)
	}
	if strings.Compare("NumericStaticMapHash", NumericStaticMapHash.String()) != 0 {
		t.Errorf("NumericStaticMapHash.String(): %s, want num", NumericStaticMapHash.String())
	}
}

func TestNumericStaticMapHashMap(t *testing.T) {
	NumericStaticMapHash, err := createNumericStaticMapHashVindex()
	if err != nil {
		t.Fatalf("failed to create vindex: %v", err)
	}
	got, err := NumericStaticMapHash.(Unique).Map(nil, []sqltypes.Value{
		sqltypes.NewInt64(1),
		sqltypes.NewInt64(2),
		sqltypes.NewInt64(3),
		sqltypes.NewInt64(4),
		sqltypes.NewInt64(5),
		sqltypes.NewInt64(6),
	})
	if err != nil {
		t.Error(err)
	}

	// in the third slice, we expect 2 instead of 3 as numeric_static_map_test.json
	// has 3 mapped to 2
	want := [][]byte{
		[]byte("\x00\x00\x00\x00\x00\x00\x00\x01"),
		[]byte("\x06\xe7\xea\"Βp\x8f"),
		[]byte("\x00\x00\x00\x00\x00\x00\x00\x02"),
		[]byte("\xd2\xfd\x88g\xd5\r-\xfe"),
		[]byte("p\xbb\x02<\x81\f\xa8z"),
		[]byte("\xf0\x98H\n\xc4ľq"),
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("NumericStaticMapHash.Map(): %+v, want %+v", got, want)
	}
}

func TestNumericStaticMapHashMapBadData(t *testing.T) {
	NumericStaticMapHash, err := createNumericStaticMapHashVindex()
	if err != nil {
		t.Fatalf("failed to create vindex: %v", err)
	}
	_, err = NumericStaticMapHash.(Unique).Map(nil, []sqltypes.Value{sqltypes.NewFloat64(1.1)})
	want := `NumericStaticMapHash.Map(): could not parse value: 1.1`
	if err == nil || err.Error() != want {
		t.Errorf("NumericStaticMapHash.Map(): %v, want %v", err, want)
	}
}

func TestNumericStaticMapHashVerify(t *testing.T) {
	NumericStaticMapHash, err := createNumericStaticMapHashVindex()
	if err != nil {
		t.Fatalf("failed to create vindex: %v", err)
	}
	got, err := NumericStaticMapHash.Verify(nil,
		[]sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2)},
		[][]byte{[]byte("\x00\x00\x00\x00\x00\x00\x00\x01"), []byte("\x00\x00\x00\x00\x00\x00\x00\x01")})
	if err != nil {
		t.Error(err)
	}
	want := []bool{true, false}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("NumericStaticMapHash.Verify(match): %v, want %v", got, want)
	}

	// Failure test
	_, err = NumericStaticMapHash.Verify(nil, []sqltypes.Value{sqltypes.NewVarBinary("aa")}, [][]byte{nil})
	wantErr := "NumericStaticMapHash.Verify(): could not parse value: aa"
	if err == nil || err.Error() != wantErr {
		t.Errorf("NumericStaticMapHash.Verify(): %v, want %s", err, wantErr)
	}
}

func TestNumericStaticMapHashReverseMap(t *testing.T) {
	got, err := hash.(Reversible).ReverseMap(nil, [][]byte{[]byte("\x16k@\xb4J\xbaK\xd6")})
	if err != nil {
		t.Error(err)
	}
	want := []sqltypes.Value{sqltypes.NewUint64(uint64(1))}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("NumericStaticMapHash.ReverseMap(): %v, want %v", got, want)
	}
}

func TestNumericStaticMapHashReverseMapNeg(t *testing.T) {
	_, err := hash.(Reversible).ReverseMap(nil, [][]byte{[]byte("\x16k@\xb4J\xbaK\xd6\x16k@\xb4J\xbaK\xd6")})
	want := "invalid keyspace id: 166b40b44aba4bd6166b40b44aba4bd6"
	if err.Error() != want {
		t.Errorf("NumericStaticMapHash.ReverseMapNeg(): %v, want %v", err, want)
	}
}
