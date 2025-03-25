package parser

import (
	"cmp"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/rfberaldo/sqlz/binds"
)

const EOF = 0

type Parser struct {
	input        string
	bind         binds.Bind
	position     int
	readPosition int
	ch           rune
	idents       []string
	identCount   int
	bindCount    int
	output       stringBuilder

	// the slice length by ident index which have an `IN` clause.
	// if there's items in this map we have to duplicate placeholder by count.
	inClauseCountByIndex map[int]int
}

type namedOptions struct {
	skipQuery  bool
	skipIdents bool
}

func (p *Parser) parseNamed(opts namedOptions) (string, []string) {
	p.read()
	p.output.skip = opts.skipQuery
	p.output.Grow(len(p.input)) // max will be len(input)

	for {
		p.skipWhitespace()
		p.tryReadIdent(opts.skipIdents)

		if p.ch == EOF {
			break
		}

		p.output.WriteRune(p.ch)
		p.read()
	}

	return p.output.String(), p.idents
}

func (p *Parser) skipWhitespace() {
	pos := p.readPosition

	for unicode.IsSpace(p.ch) {
		p.read()
	}

	if p.readPosition > pos {
		p.output.WriteRune(' ')
	}
}

func (p *Parser) read() {
	if p.readPosition >= len(p.input) {
		p.ch = EOF
		p.position = p.readPosition
		p.readPosition += 1
	} else {
		r, size := utf8.DecodeRuneInString(p.input[p.readPosition:])
		p.ch = r
		p.position = p.readPosition
		p.readPosition += size
	}
}

func (p *Parser) tryReadIdent(skipIdents bool) {
	const placeholder = ':'
	if p.ch != placeholder {
		return
	}

	// escaped placeholder, read next
	if p.peek() == placeholder {
		p.read()
		return
	}

	if !unicode.IsLetter(p.peek()) {
		return
	}

	ident := p.readIdent(isIdentChar)
	if !skipIdents {
		p.idents = append(p.idents, ident)
	}
	p.identCount++
	count := p.inClauseCountByIndex[p.identCount-1]
	count = cmp.Or(count, 1)

	for i := range count {
		p.bindCount++

		switch p.bind {
		case binds.Question:
			p.output.WriteRune('?')
		case binds.Colon:
			p.output.WriteRune(':')
			p.output.WriteString(ident)
		case binds.At:
			p.output.WriteString("@p")
			p.output.WriteString(strconv.Itoa(p.bindCount))
		case binds.Dollar:
			p.output.WriteRune('$')
			p.output.WriteString(strconv.Itoa(p.bindCount))
		}

		isLast := i == count-1
		if count > 1 && !isLast {
			p.output.WriteRune(',')
		}
	}
}

// readIdent will [read] while strategy(ch)=true.
func (p *Parser) readIdent(strategy strategyFn) string {
	p.read()
	position := p.position
	for strategy(p.ch) {
		p.read()
	}
	return p.input[position:p.position]
}

func (p *Parser) peek() rune {
	r, _ := utf8.DecodeRuneInString(p.input[p.readPosition:])
	return r
}

func (p *Parser) parseIn() string {
	p.read()
	p.output.Grow(len(p.input) + 2) // min will be len(input)+2

	for {
		p.skipWhitespace()
		p.tryReadPlaceholder()

		if p.ch == EOF {
			break
		}

		p.output.WriteRune(p.ch)
		p.read()
	}

	return p.output.String()
}

func (p *Parser) tryReadPlaceholder() {
	placeholder, readStrategy, isNumbered := getBindInfo(p.bind)

	if p.ch != rune(placeholder) {
		return
	}

	// escaped placeholder, read next
	if p.peek() == rune(placeholder) {
		p.read()
		return
	}

	var ident string
	if readStrategy != nil {
		ident = p.readIdent(readStrategy)
	} else {
		p.read()
	}
	p.identCount++
	count := p.inClauseCountByIndex[p.identCount-1]
	count = cmp.Or(count, 1)

	for i := range count {
		p.bindCount++
		p.output.WriteRune(placeholder)
		if p.bind == binds.Colon {
			p.output.WriteString(ident)
		}
		if isNumbered {
			p.output.WriteString(strconv.Itoa(p.bindCount))
		}

		isLast := i == count-1
		if count > 1 && !isLast {
			p.output.WriteRune(',')
		}
	}
}

type strategyFn = func(ch rune) bool

func getBindInfo(bind binds.Bind) (rune, strategyFn, bool) {
	var placeholder rune
	var readStrategy strategyFn
	var isNumbered bool

	switch bind {
	case binds.At:
		placeholder = '@'
		readStrategy = unicode.IsNumber
		isNumbered = true

	case binds.Dollar:
		placeholder = '$'
		readStrategy = unicode.IsNumber
		isNumbered = true

	case binds.Colon:
		placeholder = ':'
		readStrategy = isIdentChar

	case binds.Question:
		placeholder = '?'
	}

	return placeholder, readStrategy, isNumbered
}

func isIdentChar(ch rune) bool {
	return ch == '_' || ch == '.' || unicode.IsLetter(ch) || unicode.IsNumber(ch)
}

func spreadSliceValues(args ...any) (map[int]int, []any, error) {
	inClauseCountByIndex := make(map[int]int)
	outArgs := make([]any, 0, len(args))

	for i, arg := range args {
		if shouldSpread(arg) {
			refValue := reflect.ValueOf(arg)
			length := refValue.Len()
			if length == 0 {
				return nil, nil, fmt.Errorf("sqlz: empty slice passed to 'IN' clause")
			}
			inClauseCountByIndex[i] = length
			for j := range length {
				outArgs = append(outArgs, refValue.Index(j).Interface())
			}
			continue
		}

		outArgs = append(outArgs, arg)
	}

	return inClauseCountByIndex, outArgs, nil
}

func shouldSpread(arg any) bool {
	if arg == nil {
		return false
	}

	v := reflect.ValueOf(arg)
	t := v.Type()
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	// []byte is a [driver.Value] type so it should not be expanded
	if t == reflect.TypeOf([]byte{}) {
		return false
	}

	// if it's slice then it's part of 'IN' clause and have to spread
	kind := v.Kind()
	return kind == reflect.Slice || kind == reflect.Array
}

// stringBuilder is a wrapper around [strings.Builder] to skip
// processing when skipQuery=true
type stringBuilder struct {
	sb   strings.Builder
	skip bool
}

func (sb *stringBuilder) Grow(n int) {
	if sb.skip {
		return
	}
	sb.sb.Grow(n)
}

func (sb *stringBuilder) String() string {
	if sb.skip {
		return ""
	}
	return sb.sb.String()
}

func (sb *stringBuilder) WriteRune(r rune) (int, error) {
	if sb.skip {
		return 0, nil
	}
	return sb.sb.WriteRune(r)
}

func (sb *stringBuilder) WriteString(s string) (int, error) {
	if sb.skip {
		return 0, nil
	}
	return sb.sb.WriteString(s)
}
