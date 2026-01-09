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

// 's or 'S or ' (Apostrophe S or Apostrophe)
var possessiveLower = []byte{39, 115} // 's
var possessiveUpper = []byte{39, 83}  // 'S

// Unicode possessive markers: U+2019 (RIGHT SINGLE QUOTATION MARK) and U+FF07 (FULLWIDTH APOSTROPHE)
var possessiveUnicode = [][]byte{
	{0xE2, 0x80, 0x99, 0x73}, // 's (U+2019 + 's')
	{0xE2, 0x80, 0x99, 0x53}, // 'S (U+2019 + 'S')
	{0xEF, 0xBC, 0x87, 0x73}, // ＇s (U+FF07 + 's')
	{0xEF, 0xBC, 0x87, 0x53}, // ＇S (U+FF07 + 'S')
	{0xE2, 0x80, 0x99},       // ' (U+2019)
	{0xEF, 0xBC, 0x87},       // ＇ (U+FF07)
}

func PossessiveFilter(input analysis.TokenStream) analysis.TokenStream {
	rv := make(analysis.TokenStream, 0, len(input))

	for _, token := range input {
		rv = append(rv, token)
		term := token.Term

		if len(term) == 0 {
			continue
		}

		// Check for ASCII 's or 'S first
		if bytes.HasSuffix(term, possessiveLower) || bytes.HasSuffix(term, possessiveUpper) {
			token.Term = term[:len(term)-2]
			continue
		}

		// Check for Unicode possessive markers
		removed := false
		for _, unicodePossessive := range possessiveUnicode {
			if bytes.HasSuffix(term, unicodePossessive) {
				token.Term = term[:len(term)-len(unicodePossessive)]
				removed = true
				break
			}
		}

		// If no Unicode possessive found, check for ASCII apostrophe
		if !removed && len(term) > 0 && term[len(term)-1] == 39 { // '
			token.Term = term[:len(term)-1]
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
