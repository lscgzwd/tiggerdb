//  Copyright (c) 2015 Couchbase, Inc.
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

package config

import (
	// token maps
	_ "github.com/lscgzwd/tiggerdb/analysis/tokenmap"

	// fragment formatters
	_ "github.com/lscgzwd/tiggerdb/search/highlight/format/ansi"
	_ "github.com/lscgzwd/tiggerdb/search/highlight/format/html"

	// fragmenters
	_ "github.com/lscgzwd/tiggerdb/search/highlight/fragmenter/simple"

	// highlighters
	_ "github.com/lscgzwd/tiggerdb/search/highlight/highlighter/ansi"
	_ "github.com/lscgzwd/tiggerdb/search/highlight/highlighter/html"
	_ "github.com/lscgzwd/tiggerdb/search/highlight/highlighter/simple"

	// char filters
	_ "github.com/lscgzwd/tiggerdb/analysis/char/asciifolding"
	_ "github.com/lscgzwd/tiggerdb/analysis/char/html"
	_ "github.com/lscgzwd/tiggerdb/analysis/char/regexp"
	_ "github.com/lscgzwd/tiggerdb/analysis/char/zerowidthnonjoiner"

	// analyzers
	_ "github.com/lscgzwd/tiggerdb/analysis/analyzer/custom"
	_ "github.com/lscgzwd/tiggerdb/analysis/analyzer/keyword"
	_ "github.com/lscgzwd/tiggerdb/analysis/analyzer/simple"
	_ "github.com/lscgzwd/tiggerdb/analysis/analyzer/standard"
	_ "github.com/lscgzwd/tiggerdb/analysis/analyzer/web"

	// token filters
	_ "github.com/lscgzwd/tiggerdb/analysis/token/apostrophe"
	_ "github.com/lscgzwd/tiggerdb/analysis/token/camelcase"
	_ "github.com/lscgzwd/tiggerdb/analysis/token/compound"
	_ "github.com/lscgzwd/tiggerdb/analysis/token/edgengram"
	_ "github.com/lscgzwd/tiggerdb/analysis/token/elision"
	_ "github.com/lscgzwd/tiggerdb/analysis/token/keyword"
	_ "github.com/lscgzwd/tiggerdb/analysis/token/length"
	_ "github.com/lscgzwd/tiggerdb/analysis/token/lowercase"
	_ "github.com/lscgzwd/tiggerdb/analysis/token/ngram"
	_ "github.com/lscgzwd/tiggerdb/analysis/token/reverse"
	_ "github.com/lscgzwd/tiggerdb/analysis/token/shingle"
	_ "github.com/lscgzwd/tiggerdb/analysis/token/stop"
	_ "github.com/lscgzwd/tiggerdb/analysis/token/truncate"
	_ "github.com/lscgzwd/tiggerdb/analysis/token/unicodenorm"
	_ "github.com/lscgzwd/tiggerdb/analysis/token/unique"

	// tokenizers
	_ "github.com/lscgzwd/tiggerdb/analysis/tokenizer/exception"
	_ "github.com/lscgzwd/tiggerdb/analysis/tokenizer/regexp"
	_ "github.com/lscgzwd/tiggerdb/analysis/tokenizer/single"
	_ "github.com/lscgzwd/tiggerdb/analysis/tokenizer/unicode"
	_ "github.com/lscgzwd/tiggerdb/analysis/tokenizer/web"
	_ "github.com/lscgzwd/tiggerdb/analysis/tokenizer/whitespace"

	// date time parsers
	_ "github.com/lscgzwd/tiggerdb/analysis/datetime/flexible"
	_ "github.com/lscgzwd/tiggerdb/analysis/datetime/iso"
	_ "github.com/lscgzwd/tiggerdb/analysis/datetime/optional"
	_ "github.com/lscgzwd/tiggerdb/analysis/datetime/percent"
	_ "github.com/lscgzwd/tiggerdb/analysis/datetime/sanitized"
	_ "github.com/lscgzwd/tiggerdb/analysis/datetime/timestamp/microseconds"
	_ "github.com/lscgzwd/tiggerdb/analysis/datetime/timestamp/milliseconds"
	_ "github.com/lscgzwd/tiggerdb/analysis/datetime/timestamp/nanoseconds"
	_ "github.com/lscgzwd/tiggerdb/analysis/datetime/timestamp/seconds"

	// languages
	_ "github.com/lscgzwd/tiggerdb/analysis/lang/ar"
	_ "github.com/lscgzwd/tiggerdb/analysis/lang/bg"
	_ "github.com/lscgzwd/tiggerdb/analysis/lang/ca"
	_ "github.com/lscgzwd/tiggerdb/analysis/lang/cjk"
	_ "github.com/lscgzwd/tiggerdb/analysis/lang/ckb"
	_ "github.com/lscgzwd/tiggerdb/analysis/lang/cs"
	_ "github.com/lscgzwd/tiggerdb/analysis/lang/da"
	_ "github.com/lscgzwd/tiggerdb/analysis/lang/de"
	_ "github.com/lscgzwd/tiggerdb/analysis/lang/el"
	_ "github.com/lscgzwd/tiggerdb/analysis/lang/en"
	_ "github.com/lscgzwd/tiggerdb/analysis/lang/es"
	_ "github.com/lscgzwd/tiggerdb/analysis/lang/eu"
	_ "github.com/lscgzwd/tiggerdb/analysis/lang/fa"
	_ "github.com/lscgzwd/tiggerdb/analysis/lang/fi"
	_ "github.com/lscgzwd/tiggerdb/analysis/lang/fr"
	_ "github.com/lscgzwd/tiggerdb/analysis/lang/ga"
	_ "github.com/lscgzwd/tiggerdb/analysis/lang/gl"
	_ "github.com/lscgzwd/tiggerdb/analysis/lang/hi"
	_ "github.com/lscgzwd/tiggerdb/analysis/lang/hr"
	_ "github.com/lscgzwd/tiggerdb/analysis/lang/hu"
	_ "github.com/lscgzwd/tiggerdb/analysis/lang/hy"
	_ "github.com/lscgzwd/tiggerdb/analysis/lang/id"
	_ "github.com/lscgzwd/tiggerdb/analysis/lang/in"
	_ "github.com/lscgzwd/tiggerdb/analysis/lang/it"
	_ "github.com/lscgzwd/tiggerdb/analysis/lang/nl"
	_ "github.com/lscgzwd/tiggerdb/analysis/lang/no"
	_ "github.com/lscgzwd/tiggerdb/analysis/lang/pl"
	_ "github.com/lscgzwd/tiggerdb/analysis/lang/pt"
	_ "github.com/lscgzwd/tiggerdb/analysis/lang/ro"
	_ "github.com/lscgzwd/tiggerdb/analysis/lang/ru"
	_ "github.com/lscgzwd/tiggerdb/analysis/lang/sv"
	_ "github.com/lscgzwd/tiggerdb/analysis/lang/tr"

	// kv stores
	_ "github.com/lscgzwd/tiggerdb/index/upsidedown/store/boltdb"
	_ "github.com/lscgzwd/tiggerdb/index/upsidedown/store/goleveldb"
	_ "github.com/lscgzwd/tiggerdb/index/upsidedown/store/gtreap"
	_ "github.com/lscgzwd/tiggerdb/index/upsidedown/store/moss"

	// index types
	_ "github.com/lscgzwd/tiggerdb/index/upsidedown"
)
