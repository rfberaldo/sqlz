package parser

import (
	"cmp"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

const EOF = 0

// Bind represent the placeholder used by different drivers.
type Bind uint8

const (
	BindUnknown  Bind = iota
	BindAt            // placeholder '@p1'
	BindColon         // placeholder ':name'
	BindDollar        // placeholder '$1'
	BindQuestion      // placeholder '?'
)

// Parser is an SQL query parser mostly for named queries.
type Parser struct {
	input        string
	bind         Bind
	position     int
	readPosition int
	ch           rune
	idents       []string
	identCount   int
	bindCount    int
	output       strings.Builder

	// the slice length by ident index which have an "IN" clause.
	// if there's items in this map we have to duplicate placeholder by count.
	inClauseCountByIndex map[int]int
}

func (p *Parser) parse(skipIdents bool) (string, []string) {
	p.read()
	p.output.Grow(len(p.input)) // max will be len(input)

	for {
		p.skipWhitespace()
		p.tryReadIdent(skipIdents)

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
		case BindQuestion:
			p.output.WriteRune('?')
		case BindColon:
			p.output.WriteRune(':')
			p.output.WriteString(ident)
		case BindAt:
			p.output.WriteString("@p")
			p.output.WriteString(strconv.Itoa(p.bindCount))
		case BindDollar:
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
func (p *Parser) readIdent(strategy strategyFunc) string {
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

func (p *Parser) parseInNative() string {
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
		if p.bind == BindAt {
			p.output.WriteByte('p')
		}
		if p.bind == BindColon {
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

type strategyFunc = func(ch rune) bool

func getBindInfo(bind Bind) (rune, strategyFunc, bool) {
	var placeholder rune
	var readStrategy strategyFunc
	var isNumbered bool

	switch bind {
	case BindAt:
		placeholder = '@'
		readStrategy = isIdentChar
		isNumbered = true

	case BindDollar:
		placeholder = '$'
		readStrategy = unicode.IsNumber
		isNumbered = true

	case BindColon:
		placeholder = ':'
		readStrategy = isIdentChar

	case BindQuestion:
		placeholder = '?'
	}

	return placeholder, readStrategy, isNumbered
}

func isIdentChar(ch rune) bool {
	return ch == '_' || ch == '.' || unicode.IsLetter(ch) || unicode.IsNumber(ch)
}
