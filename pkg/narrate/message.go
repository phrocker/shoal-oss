// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.

package narrate

import (
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// The message syntax is a subset of ICU MessageFormat:
//
//	{name}                          a Fragment, number, time or duration
//	{name, number}                  an integer or a float
//	{name, time}                    a time.Time, always rendered in UTC
//	{name, duration}                a time.Duration
//	{name, list}  {name, list, or}  a []Fragment joined with "and" / "or"
//	{name, plural, =0 {…} one {…} other {…}}   # is the formatted count
//	{name, select, a {…} other {…}}            on a Selector
//
// Apostrophes follow ICU's DOUBLE_OPTIONAL rule: '' is a literal apostrophe,
// and an apostrophe immediately before {, } or (inside plural) # starts a
// quoted literal that runs to the next single apostrophe. Any other apostrophe
// is literal, so ordinary English needs no escaping.
//
// A pattern is parsed once, when its catalog is loaded. Argument values are
// inserted into the parsed tree and never re-parsed, so braces, apostrophes or
// # inside a value are text, not syntax.

type node interface{ isNode() }

type textNode struct{ text string }
type argNode struct {
	name, kind, style string
}
type hashNode struct{}
type choiceNode struct {
	name   string
	plural bool
	exact  map[int64][]node
	cases  map[string][]node
}

func (textNode) isNode()   {}
func (argNode) isNode()    {}
func (hashNode) isNode()   {}
func (choiceNode) isNode() {}

// Selector is a select argument. It only chooses a branch and is never
// printed, so it may carry any value; an unknown value takes "other".
type Selector string

const maxPatternDepth = 8

type patternParser struct {
	src []rune
	pos int
}

func parsePattern(src string) ([]node, error) {
	if !utf8.ValidString(src) {
		return nil, fmt.Errorf("pattern is not valid UTF-8")
	}
	p := &patternParser{src: []rune(src)}
	nodes, err := p.pattern(false, 0)
	if err != nil {
		return nil, err
	}
	if p.pos != len(p.src) {
		return nil, fmt.Errorf("unexpected %q at %d", p.src[p.pos], p.pos)
	}
	return nodes, nil
}

func (p *patternParser) pattern(inPlural bool, depth int) ([]node, error) {
	if depth > maxPatternDepth {
		return nil, fmt.Errorf("pattern nests too deeply")
	}
	var nodes []node
	var text strings.Builder
	flush := func() {
		if text.Len() > 0 {
			nodes = append(nodes, textNode{text.String()})
			text.Reset()
		}
	}
	for p.pos < len(p.src) {
		r := p.src[p.pos]
		switch {
		case r == '\'':
			p.pos++
			if p.pos < len(p.src) && p.src[p.pos] == '\'' {
				text.WriteRune('\'')
				p.pos++
				continue
			}
			if p.pos < len(p.src) && (p.src[p.pos] == '{' || p.src[p.pos] == '}' ||
				(inPlural && p.src[p.pos] == '#')) {
				for {
					if p.pos >= len(p.src) {
						return nil, fmt.Errorf("unterminated quoted literal")
					}
					if p.src[p.pos] == '\'' {
						if p.pos+1 < len(p.src) && p.src[p.pos+1] == '\'' {
							text.WriteRune('\'')
							p.pos += 2
							continue
						}
						p.pos++
						break
					}
					text.WriteRune(p.src[p.pos])
					p.pos++
				}
				continue
			}
			text.WriteRune('\'')
		case r == '{':
			flush()
			p.pos++
			n, err := p.argument(depth)
			if err != nil {
				return nil, err
			}
			nodes = append(nodes, n)
		case r == '}':
			flush()
			return nodes, nil
		case r == '#' && inPlural:
			flush()
			nodes = append(nodes, hashNode{})
			p.pos++
		default:
			text.WriteRune(r)
			p.pos++
		}
	}
	flush()
	return nodes, nil
}

func (p *patternParser) spaces() {
	for p.pos < len(p.src) && (p.src[p.pos] == ' ' || p.src[p.pos] == '\n' || p.src[p.pos] == '\t') {
		p.pos++
	}
}

func (p *patternParser) word() string {
	start := p.pos
	for p.pos < len(p.src) {
		r := p.src[p.pos]
		if r == '_' || r == '=' || r == '-' || r == '.' || (r >= 'a' && r <= 'z') ||
			(r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			p.pos++
			continue
		}
		break
	}
	return string(p.src[start:p.pos])
}

func (p *patternParser) expect(r rune) error {
	p.spaces()
	if p.pos >= len(p.src) || p.src[p.pos] != r {
		return fmt.Errorf("expected %q at %d", r, p.pos)
	}
	p.pos++
	return nil
}

func (p *patternParser) argument(depth int) (node, error) {
	p.spaces()
	name := p.word()
	if name == "" {
		return nil, fmt.Errorf("argument name expected at %d", p.pos)
	}
	p.spaces()
	if p.pos < len(p.src) && p.src[p.pos] == '}' {
		p.pos++
		return argNode{name: name}, nil
	}
	if err := p.expect(','); err != nil {
		return nil, err
	}
	p.spaces()
	kind := p.word()
	switch kind {
	case "plural", "select":
		if err := p.expect(','); err != nil {
			return nil, err
		}
		choice := choiceNode{
			name: name, plural: kind == "plural",
			exact: map[int64][]node{}, cases: map[string][]node{},
		}
		for {
			p.spaces()
			if p.pos < len(p.src) && p.src[p.pos] == '}' {
				p.pos++
				break
			}
			selector := p.word()
			if selector == "" {
				return nil, fmt.Errorf("selector expected at %d", p.pos)
			}
			if err := p.expect('{'); err != nil {
				return nil, err
			}
			body, err := p.pattern(choice.plural, depth+1)
			if err != nil {
				return nil, err
			}
			if err := p.expect('}'); err != nil {
				return nil, err
			}
			if strings.HasPrefix(selector, "=") {
				if !choice.plural {
					return nil, fmt.Errorf("exact selector %q outside plural", selector)
				}
				value, err := strconv.ParseInt(selector[1:], 10, 64)
				if err != nil {
					return nil, fmt.Errorf("bad exact selector %q", selector)
				}
				if _, dup := choice.exact[value]; dup {
					return nil, fmt.Errorf("duplicate selector %q", selector)
				}
				choice.exact[value] = body
				continue
			}
			if choice.plural && !pluralKeyword(selector) {
				return nil, fmt.Errorf("unknown plural category %q", selector)
			}
			if _, dup := choice.cases[selector]; dup {
				return nil, fmt.Errorf("duplicate selector %q", selector)
			}
			choice.cases[selector] = body
		}
		if _, ok := choice.cases["other"]; !ok {
			return nil, fmt.Errorf("%s argument %q has no other case", kind, name)
		}
		return choice, nil
	case "number", "time", "duration", "list":
		style := ""
		p.spaces()
		if p.pos < len(p.src) && p.src[p.pos] == ',' {
			p.pos++
			p.spaces()
			style = p.word()
		}
		if kind == "list" && style != "" && style != "and" && style != "or" {
			return nil, fmt.Errorf("unknown list style %q", style)
		}
		if kind != "list" && style != "" {
			return nil, fmt.Errorf("%s takes no style", kind)
		}
		if err := p.expect('}'); err != nil {
			return nil, err
		}
		return argNode{name: name, kind: kind, style: style}, nil
	default:
		return nil, fmt.Errorf("unknown argument type %q", kind)
	}
}

func pluralKeyword(s string) bool {
	switch s {
	case "zero", "one", "two", "few", "many", "other":
		return true
	}
	return false
}

// Args are the values a message is formatted with. Only package types and
// numbers are accepted: a bare Go string is refused, so no text reaches a
// sentence except through a constructor that decided how to present it.
type Args map[string]any

// argUses maps every argument a pattern reads to the kinds it is read as:
// "" for a simple argument, "number", "time", "duration", "list", "plural"
// or "select".
func argUses(nodes []node, into map[string]map[string]bool) {
	use := func(name, kind string) {
		if into[name] == nil {
			into[name] = map[string]bool{}
		}
		into[name][kind] = true
	}
	for _, n := range nodes {
		switch n := n.(type) {
		case argNode:
			use(n.name, n.kind)
		case choiceNode:
			if n.plural {
				use(n.name, "plural")
			} else {
				use(n.name, "select")
			}
			for _, body := range n.exact {
				argUses(body, into)
			}
			for _, body := range n.cases {
				argUses(body, into)
			}
		}
	}
}

// compatibleUse reports whether a translation may read an argument as kind,
// given the kinds the English message reads it as. The English message is
// the contract for what the renderer passes:
//
//   - the same kind is always compatible;
//   - an argument English reads as a plural is an integer, so a translation
//     may also read it as a number;
//   - a plural is allowed only where English reads the argument as a plural,
//     because "number" also carries probabilities, and a float cannot select
//     a plural form;
//   - a simple reference prints any value except a list or a selector.
func compatibleUse(kind string, english map[string]bool) bool {
	if english[kind] {
		return true
	}
	switch kind {
	case "number":
		return english["plural"]
	case "":
		for e := range english {
			if e != "list" && e != "select" {
				return true
			}
		}
	}
	return false
}

type formatter struct {
	catalog *Catalog
	out     strings.Builder
	quotes  []Quote
	spans   []Span
}

func (f *formatter) run(nodes []node, args Args, count *string, depth int) error {
	if depth > maxPatternDepth*2 {
		return fmt.Errorf("message nests too deeply")
	}
	for _, n := range nodes {
		switch n := n.(type) {
		case textNode:
			f.out.WriteString(n.text)
		case hashNode:
			if count == nil {
				return fmt.Errorf("# outside plural")
			}
			f.out.WriteString(*count)
		case argNode:
			value, ok := args[n.name]
			if !ok {
				return fmt.Errorf("missing argument %q", n.name)
			}
			if err := f.argument(n, value, depth); err != nil {
				return fmt.Errorf("argument %q: %w", n.name, err)
			}
		case choiceNode:
			value, ok := args[n.name]
			if !ok {
				return fmt.Errorf("missing argument %q", n.name)
			}
			if n.plural {
				c, ok := asInt(value)
				if !ok {
					return fmt.Errorf("plural argument %q is %T", n.name, value)
				}
				body, ok := n.exact[c]
				if !ok {
					body = n.cases[f.catalog.pluralCategory(c)]
					if body == nil {
						body = n.cases["other"]
					}
				}
				shown := f.catalog.integer(c)
				if u, isUnsigned := value.(uint64); isUnsigned {
					shown = f.catalog.unsigned(u)
				}
				if err := f.run(body, args, &shown, depth+1); err != nil {
					return err
				}
				continue
			}
			s, ok := value.(Selector)
			if !ok {
				return fmt.Errorf("select argument %q is %T", n.name, value)
			}
			body, ok := n.cases[string(s)]
			if !ok {
				body = n.cases["other"]
			}
			if err := f.run(body, args, count, depth+1); err != nil {
				return err
			}
		}
	}
	return nil
}

func asInt(value any) (int64, bool) {
	switch v := value.(type) {
	case int:
		return int64(v), true
	case int64:
		return v, true
	case uint64:
		if v > 1<<62 {
			return 1 << 62, true
		}
		return int64(v), true
	}
	return 0, false
}

func (f *formatter) fragment(fr Fragment) {
	offset := f.out.Len()
	f.out.WriteString(fr.text)
	f.quotes = append(f.quotes, fr.quotes...)
	for _, span := range fr.spans {
		span.Start += offset
		span.End += offset
		f.spans = append(f.spans, span)
	}
}

func (f *formatter) argument(n argNode, value any, depth int) error {
	switch n.kind {
	case "":
		switch v := value.(type) {
		case Fragment:
			f.fragment(v)
			return nil
		case time.Time:
			f.out.WriteString(f.catalog.time(v))
			return nil
		case time.Duration:
			fr, err := f.catalog.duration(v)
			if err != nil {
				return err
			}
			f.fragment(fr)
			return nil
		case float64:
			f.out.WriteString(f.catalog.decimal(v))
			return nil
		case uint64:
			f.out.WriteString(f.catalog.unsigned(v))
			return nil
		}
		if i, ok := asInt(value); ok {
			f.out.WriteString(f.catalog.integer(i))
			return nil
		}
	case "number":
		if v, ok := value.(float64); ok {
			f.out.WriteString(f.catalog.decimal(v))
			return nil
		}
		if v, ok := value.(uint64); ok {
			f.out.WriteString(f.catalog.unsigned(v))
			return nil
		}
		if i, ok := asInt(value); ok {
			f.out.WriteString(f.catalog.integer(i))
			return nil
		}
	case "time":
		if v, ok := value.(time.Time); ok {
			f.out.WriteString(f.catalog.time(v))
			return nil
		}
	case "duration":
		if v, ok := value.(time.Duration); ok {
			fr, err := f.catalog.duration(v)
			if err != nil {
				return err
			}
			f.fragment(fr)
			return nil
		}
	case "list":
		if v, ok := value.([]Fragment); ok {
			fr, err := f.catalog.list(v, n.style)
			if err != nil {
				return err
			}
			f.fragment(fr)
			return nil
		}
	}
	return fmt.Errorf("value of type %T cannot fill a %q argument", value, n.kind)
}
