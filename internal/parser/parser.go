package parser

import (
	"cmp"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

type Parser struct {
	input        string
	bind         Bind
	position     int
	readPosition int
	ch           byte
	idents       []string
	identCount   int
	bindCount    int
	output       strings.Builder // strings.Builder is more performant than string
	eof          bool

	// the slice length by ident index which have an `IN` clause.
	// if there's items in this map we have to duplicate ident by count.
	inClauseCountByIndex map[int]int
}

func (p *Parser) parseNamed() (string, []string) {
	for {
		p.skipWhitespace()
		p.tryReadIdent()

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

func (p *Parser) tryReadIdent() {
	if p.ch != ':' {
		return
	}

	// escaped colon (::), advance one char
	if p.peekChar() == ':' {
		p.readChar()
		return
	}

	if !isLetter(p.peekChar()) {
		return
	}

	ident := p.readIdent()
	p.idents = append(p.idents, ident)
	// we use len(idents)-1 instead of bindCount because we need the original index.
	// if we're running ParseInClause we've already parsed before.
	count := p.inClauseCountByIndex[len(p.idents)-1]
	count = cmp.Or(count, 1)

	for i := range count {
		p.bindCount++

		switch p.bind {
		case BindQuestion:
			p.output.WriteByte('?')
		case BindColon:
			p.output.WriteByte(':')
			p.output.WriteString(ident)
		case BindAt:
			p.output.WriteString("@p")
			p.output.WriteString(strconv.Itoa(p.bindCount))
		case BindDollar:
			p.output.WriteByte('$')
			p.output.WriteString(strconv.Itoa(p.bindCount))
		}

		isLast := i == count-1
		if count > 1 && !isLast {
			p.output.WriteByte(',')
		}
	}
}

func (p *Parser) readIdent() string {
	p.readChar()
	position := p.position
	for isIdentChar(p.ch) {
		if p.eof {
			break
		}
		p.readChar()
	}
	return p.input[position:p.position]
}

func (p *Parser) peekChar() byte {
	if p.readPosition >= len(p.input) {
		return 0
	} else {
		return p.input[p.readPosition]
	}
}

func isLetter(ch byte) bool {
	return 'a' <= ch && ch <= 'z' || 'A' <= ch && ch <= 'Z'
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

func (p *Parser) parseIn() string {
	for {
		p.skipWhitespace()
		p.tryReadBind()

		if p.eof {
			break
		}

		p.output.WriteByte(p.ch)
		p.readChar()
	}

	output := strings.TrimSuffix(p.output.String(), ";")

	return output
}

func (p *Parser) tryReadBind() {
	if p.bind != BindQuestion {
		return
	}

	if p.ch != '?' {
		return
	}

	// escaped question (??), advance one char
	if p.peekChar() == '?' {
		p.readChar()
		return
	}

	p.readChar()
	p.identCount++
	p.bindCount++
	count := p.inClauseCountByIndex[p.identCount-1] - 1

	p.output.WriteByte('?')
	for range count {
		p.bindCount++
		p.output.WriteString(",?")
	}
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
