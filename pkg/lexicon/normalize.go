/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package lexicon

import (
	"sync"
	"unicode"
	"unicode/utf8"

	"golang.org/x/text/cases"
	"golang.org/x/text/unicode/norm"
)

// NormalizationVersion identifies the tokenization contract below. It is
// written into every bundle, so changing the contract changes every bundle ID.
// Bump it whenever Tokenize can return a different result for some input.
const NormalizationVersion uint32 = 1

// unicodeTables names the Unicode data the normalization depends on. The x/text
// tables are selected by Go toolchain version, so the same source can normalize
// differently under two toolchains; recording the versions in the bundle makes
// such a change visible as a different bundle ID instead of a silent mismatch.
var unicodeTables = "norm=" + norm.Version +
	";cases=" + cases.UnicodeVersion +
	";unicode=" + unicode.Version

// Token is one normalized token and the byte span of the original text it was
// produced from. A span covers whole normalization segments, so a token produced
// from part of a compatibility expansion (for example "1" from "⑴") spans the
// whole source character.
type Token struct {
	Text  string
	Start int
	End   int
}

var foldPool = sync.Pool{New: func() any {
	caser := cases.Fold()
	return &caser
}}

// Tokenize applies NFKC, full case folding and NFKC again (the NFKC_Casefold
// shape), then splits on maximal runs of letters, numbers and combining marks.
// Everything else separates tokens. Folding is locale-independent: Turkish
// dotted capital I folds to "i" plus a combining dot, not to "i".
// Camel case is not split.
func Tokenize(text string) []Token {
	var tokens []Token
	current := make([]byte, 0, 32)
	start, end := -1, 0
	emit := func() {
		if start >= 0 {
			tokens = append(tokens, Token{Text: string(current), Start: start, End: end})
			current = current[:0]
			start = -1
		}
	}
	var caser *cases.Caser
	defer func() {
		if caser != nil {
			foldPool.Put(caser)
		}
	}()
	for index := 0; index < len(text); {
		c := text[index]
		if c < utf8.RuneSelf && (index+1 == len(text) || text[index+1] < utf8.RuneSelf) {
			// An ASCII byte followed by ASCII (or the end) is its own NFKC
			// segment and is unchanged by NFKC; full folding only lowercases.
			switch {
			case 'a' <= c && c <= 'z', '0' <= c && c <= '9':
			case 'A' <= c && c <= 'Z':
				c += 'a' - 'A'
			default:
				emit()
				index++
				continue
			}
			if start < 0 {
				start = index
			}
			current = append(current, c)
			end = index + 1
			index++
			continue
		}
		size := norm.NFKC.NextBoundaryInString(text[index:], true)
		if size <= 0 {
			size = len(text) - index
		}
		if caser == nil {
			caser = foldPool.Get().(*cases.Caser)
		}
		caser.Reset()
		segment := norm.NFKC.String(
			caser.String(norm.NFKC.String(text[index : index+size])))
		for _, r := range segment {
			if !wordRune(r) {
				emit()
				continue
			}
			if start < 0 {
				start = index
			}
			current = utf8.AppendRune(current, r)
			end = index + size
		}
		index += size
	}
	emit()
	return tokens
}

func wordRune(r rune) bool {
	if r == utf8.RuneError {
		return false
	}
	return unicode.IsLetter(r) || unicode.IsNumber(r) || unicode.Is(unicode.M, r)
}

// tokenTexts returns only the normalized token strings.
func tokenTexts(text string) []string {
	tokens := Tokenize(text)
	texts := make([]string, len(tokens))
	for index, token := range tokens {
		texts[index] = token.Text
	}
	return texts
}
