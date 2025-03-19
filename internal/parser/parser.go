package parser

import (
	"cmp"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/rfberaldo/sqlz/binds"
)

type Parser struct {
	input        string
	bind         binds.Bind
	position     int
	readPosition int
	ch           byte
	idents       []string
	identCount   int
	bindCount    int
	output       stringBuilder
	eof          bool

	// the slice length by ident index which have an `IN` clause.
	// if there's items in this map we have to duplicate placeholder by count.
	inClauseCountByIndex map[int]int
}

type namedOptions struct {
	skipQuery  bool
	skipIdents bool
}

func (p *Parser) parseNamed(opts namedOptions) (string, []string) {
	p.readChar()
	p.output.skip = opts.skipQuery
	p.output.Grow(len(p.input)) // max will be len(input)

	for {
		p.skipWhitespace()
		p.tryReadIdent(opts.skipIdents)

		if p.eof {
			break
		}

		p.output.WriteByte(p.ch)
		p.readChar()
	}

	output := strings.TrimSuffix(p.output.String(), ";")

	return output, p.idents
}

func (p *Parser) skipWhitespace() {
	pos := p.readPosition

	for isWhitespace(p.ch) {
		p.readChar()
	}

	if p.readPosition > pos {
		p.output.WriteByte(' ')
	}
}

func (p *Parser) readChar() {
	if p.readPosition >= len(p.input) {
		p.eof = true
	} else {
		p.ch = p.input[p.readPosition]
	}

	p.position = p.readPosition
	p.readPosition += 1
}

func (p *Parser) tryReadIdent(skipIdents bool) {
	const placeholder = ':'
	if p.ch != placeholder {
		return
	}

	// escaped placeholder, advance one char
	if p.peekChar() == placeholder {
		p.readChar()
		return
	}

	if !isLetter(p.peekChar()) {
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
			p.output.WriteByte('?')
		case binds.Colon:
			p.output.WriteByte(':')
			p.output.WriteString(ident)
		case binds.At:
			p.output.WriteString("@p")
			p.output.WriteString(strconv.Itoa(p.bindCount))
		case binds.Dollar:
			p.output.WriteByte('$')
			p.output.WriteString(strconv.Itoa(p.bindCount))
		}

		isLast := i == count-1
		if count > 1 && !isLast {
			p.output.WriteByte(',')
		}
	}
}

// readIdent will readChar while strategy(ch)=true or EOF.
func (p *Parser) readIdent(strategy func(ch byte) bool) string {
	p.readChar()
	position := p.position
	for strategy(p.ch) {
		if p.eof {
			break
		}
		p.readChar()
	}
	return p.input[position:p.position]
}

func (p *Parser) peekChar() byte {
	return p.input[p.readPosition]
}

func (p *Parser) parseIn() string {
	p.readChar()
	p.output.Grow(len(p.input) + 2) // min will be len(input)+2

	for {
		p.skipWhitespace()
		p.tryReadPlaceholder()

		if p.eof {
			break
		}

		p.output.WriteByte(p.ch)
		p.readChar()
	}

	output := strings.TrimSuffix(p.output.String(), ";")

	return output
}

func (p *Parser) tryReadPlaceholder() {
	placeholder, readStrategy, isNumbered := getBindInfo(p.bind)

	if p.ch != placeholder {
		return
	}

	// escaped placeholder, advance one char
	if p.peekChar() == placeholder {
		p.readChar()
		return
	}

	var ident string
	if readStrategy != nil {
		ident = p.readIdent(readStrategy)
	} else {
		p.readChar()
	}
	p.identCount++
	count := p.inClauseCountByIndex[p.identCount-1]
	count = cmp.Or(count, 1)

	for i := range count {
		p.bindCount++
		p.output.WriteByte(placeholder)
		if p.bind == binds.Colon {
			p.output.WriteString(ident)
		}
		if isNumbered {
			p.output.WriteString(strconv.Itoa(p.bindCount))
		}

		isLast := i == count-1
		if count > 1 && !isLast {
			p.output.WriteByte(',')
		}
	}
}

func getBindInfo(bind binds.Bind) (byte, func(ch byte) bool, bool) {
	var placeholder byte
	var readStrategy func(ch byte) bool
	var isNumbered bool

	switch bind {
	case binds.At:
		placeholder = '@'
		readStrategy = isNumber
		isNumbered = true

	case binds.Dollar:
		placeholder = '$'
		readStrategy = isNumber
		isNumbered = true

	case binds.Colon:
		placeholder = ':'
		readStrategy = isIdentChar

	case binds.Question:
		placeholder = '?'
	}

	return placeholder, readStrategy, isNumbered
}

func isLetter(ch byte) bool {
	return 'a' <= ch && ch <= 'z' || 'A' <= ch && ch <= 'Z'
}

func isNumber(ch byte) bool {
	return '0' <= ch && ch <= '9'
}

func isIdentChar(ch byte) bool {
	return ch == '_' || ch == '.' ||
		'a' <= ch && ch <= 'z' ||
		'A' <= ch && ch <= 'Z' ||
		'0' <= ch && ch <= '9'
}

func isWhitespace(ch byte) bool {
	return ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r'
}

func spreadSliceValues(args ...any) (map[int]int, []any, error) {
	inClauseCountByIndex := make(map[int]int)
	outArgs := make([]any, 0, len(args))

	for i, arg := range args {
		refValue := reflect.ValueOf(arg)
		switch refValue.Kind() {
		case reflect.Array, reflect.Slice:
			// if it's slice then it's part of `IN` clause and have to spread
			length := refValue.Len()
			if length == 0 {
				return nil, nil, fmt.Errorf("sqlz: empty slice passed to 'IN' clause")
			}
			inClauseCountByIndex[i] = length
			for j := range length {
				outArgs = append(outArgs, refValue.Index(j).Interface())
			}
		default:
			outArgs = append(outArgs, arg)
		}
	}

	return inClauseCountByIndex, outArgs, nil
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

func (sb *stringBuilder) WriteByte(c byte) error {
	if sb.skip {
		return nil
	}
	return sb.sb.WriteByte(c)
}

func (sb *stringBuilder) WriteString(s string) (int, error) {
	if sb.skip {
		return 0, nil
	}
	return sb.sb.WriteString(s)
}
