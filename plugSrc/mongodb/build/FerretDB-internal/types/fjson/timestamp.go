// Copyright 2021 FerretDB Inc.
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

package fjson

import (
	"encoding/json"

	"github.com/40t/go-sniffer/plugSrc/mongodb/build/FerretDB-internal/types"
	"github.com/40t/go-sniffer/plugSrc/mongodb/build/FerretDB-internal/util/lazyerrors"
)

// timestampType represents BSON Timestamp type.
type timestampType types.Timestamp

// fjsontype implements fjsontype interface.
func (ts *timestampType) fjsontype() {}

// timestampJSON is a JSON object representation of the timestampType.
type timestampJSON struct {
	T uint64 `json:"$t,string"`
}

// MarshalJSON implements fjsontype interface.
func (ts *timestampType) MarshalJSON() ([]byte, error) {
	res, err := json.Marshal(timestampJSON{
		T: uint64(*ts),
	})
	if err != nil {
		return nil, lazyerrors.Error(err)
	}
	return res, nil
}

// check interfaces
var (
	_ fjsontype = (*timestampType)(nil)
)
