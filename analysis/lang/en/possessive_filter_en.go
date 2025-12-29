// Copyright (c) 2014 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 		http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package en

import (
	"bytes"

	"github.com/lscgzwd/tiggerdb/analysis"
	"github.com/lscgzwd/tiggerdb/registry"
)

const Name = "possessive_en"

// PossessiveName is the same as Name
const PossessiveName = Name

// 's or ' (Apostrophe S or Apostrophe)
var possessive = []byte{39, 115}

func PossessiveFilter(input analysis.TokenStream) analysis.TokenStream {
	rv := make(analysis.TokenStream, 0, len(input))

	for _, token := range input {
		rv = append(rv, token)
		// if token ends in 's remove the 's
		if bytes.HasSuffix(token.Term, possessive) {
			token.Term = token.Term[:len(token.Term)-2]
		} else if token.Term[len(token.Term)-1] == 39 { // '
			token.Term = token.Term[:len(token.Term)-1]
		}
	}

	return rv
}

type possessiveFilter struct{}

func (f *possessiveFilter) Filter(input analysis.TokenStream) analysis.TokenStream {
	return PossessiveFilter(input)
}

func init() {
	err := registry.RegisterTokenFilter(Name, func(config map[string]interface{}, cache *registry.Cache) (analysis.TokenFilter, error) {
		return &possessiveFilter{}, nil
	})
	if err != nil {
		panic(err)
	}
}
