//  Copyright (c) 2016 Couchbase, Inc.
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

package cjk

import (
	"reflect"
	"testing"

	"github.com/lscgzwd/tiggerdb/analysis"
)

func TestCJKWidthFilter(t *testing.T) {

	tests := []struct {
		input  analysis.TokenStream
		output analysis.TokenStream
	}{
		{
			input: analysis.TokenStream{
				&analysis.Token{
					Term: []byte("Ｔｅｓｔ"),
				},
				&analysis.Token{
					Term: []byte("１２３４"),
				},
			},
			output: analysis.TokenStream{
				&analysis.Token{
					Term: []byte("Test"),
				},
				&analysis.Token{
					Term: []byte("1234"),
				},
			},
		},
		{
			input: analysis.TokenStream{
				&analysis.Token{
					Term: []byte("ｶﾀｶﾅ"),
				},
			},
			output: analysis.TokenStream{
				&analysis.Token{
					Term: []byte("カタカナ"),
				},
			},
		},
		{
			input: analysis.TokenStream{
				&analysis.Token{
					Term: []byte("ｳﾞｨｯ�?),
				},
			},
			output: analysis.TokenStream{
				&analysis.Token{
					Term: []byte("ヴィッツ"),
				},
			},
		},
		{
			input: analysis.TokenStream{
				&analysis.Token{
					Term: []byte("ﾊﾟﾅｿﾆｯ�?),
				},
			},
			output: analysis.TokenStream{
				&analysis.Token{
					Term: []byte("パナソニック"),
				},
			},
		},
	}

	for _, test := range tests {
		cjkWidthFilter := NewCJKWidthFilter()
		actual := cjkWidthFilter.Filter(test.input)
		if !reflect.DeepEqual(actual, test.output) {
			t.Errorf("expected %s, got %s", test.output, actual)
		}
	}
}
