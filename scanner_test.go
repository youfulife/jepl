package epl_test

import (
	"reflect"
	"strings"
	"testing"

	"epl"
)

// errstring converts an error to its string representation.
func errString(err error) string {
	if err != nil {
		return err.Error()
	}
	return ""
}

// Ensure the scanner can scan tokens correctly.
func TestScanner_Scan(t *testing.T) {
	var tests = []struct {
		s   string
		tok epl.Token
		lit string
		pos epl.Pos
	}{
		// Special tokens (EOF, ILLEGAL, WS)
		{s: ``, tok: epl.EOF},
		{s: `#`, tok: epl.ILLEGAL, lit: `#`},
		{s: ` `, tok: epl.WS, lit: " "},
		{s: "\t", tok: epl.WS, lit: "\t"},
		{s: "\n", tok: epl.WS, lit: "\n"},
		{s: "\r", tok: epl.WS, lit: "\n"},
		{s: "\r\n", tok: epl.WS, lit: "\n"},
		{s: "\rX", tok: epl.WS, lit: "\n"},
		{s: "\n\r", tok: epl.WS, lit: "\n\n"},
		{s: " \n\t \r\n\t", tok: epl.WS, lit: " \n\t \n\t"},
		{s: " foo", tok: epl.WS, lit: " "},

		// Numeric operators
		{s: `+`, tok: epl.ADD},
		{s: `-`, tok: epl.SUB},
		{s: `*`, tok: epl.MUL},
		{s: `/`, tok: epl.DIV},

		// Logical operators
		{s: `AND`, tok: epl.AND},
		{s: `and`, tok: epl.AND},
		{s: `OR`, tok: epl.OR},
		{s: `or`, tok: epl.OR},

		{s: `=`, tok: epl.EQ},
		{s: `<>`, tok: epl.NEQ},
		{s: `! `, tok: epl.ILLEGAL, lit: "!"},
		{s: `<`, tok: epl.LT},
		{s: `<=`, tok: epl.LTE},
		{s: `>`, tok: epl.GT},
		{s: `>=`, tok: epl.GTE},

		// Misc tokens
		{s: `[`, tok: epl.LBRACKET},
		{s: `(`, tok: epl.LPAREN},
		{s: `]`, tok: epl.RBRACKET},
		{s: `)`, tok: epl.RPAREN},
		{s: `,`, tok: epl.COMMA},
		{s: `;`, tok: epl.SEMICOLON},
		{s: `.`, tok: epl.DOT},
		{s: `=~`, tok: epl.EQREGEX},
		{s: `!~`, tok: epl.NEQREGEX},
		{s: `:`, tok: epl.COLON},
		{s: `::`, tok: epl.DOUBLECOLON},

		// Identifiers
		{s: `foo`, tok: epl.IDENT, lit: `foo`},
		{s: `_foo`, tok: epl.IDENT, lit: `_foo`},
		{s: `Zx12_3U_-`, tok: epl.IDENT, lit: `Zx12_3U_`},
		{s: `"foo"`, tok: epl.IDENT, lit: `foo`},
		{s: `"foo\\bar"`, tok: epl.IDENT, lit: `foo\bar`},
		{s: `"foo\bar"`, tok: epl.BADESCAPE, lit: `\b`, pos: epl.Pos{Line: 0, Char: 5}},
		{s: `"foo\"bar\""`, tok: epl.IDENT, lit: `foo"bar"`},
		{s: `test"`, tok: epl.BADSTRING, lit: "", pos: epl.Pos{Line: 0, Char: 3}},
		{s: `"test`, tok: epl.BADSTRING, lit: `test`},
		{s: `$host`, tok: epl.BOUNDPARAM, lit: `$host`},
		{s: `$"host param"`, tok: epl.BOUNDPARAM, lit: `$host param`},

		{s: `true`, tok: epl.TRUE},
		{s: `false`, tok: epl.FALSE},

		// Strings
		{s: `'testing 123!'`, tok: epl.STRING, lit: `testing 123!`},
		{s: `'foo\nbar'`, tok: epl.STRING, lit: "foo\nbar"},
		{s: `'foo\\bar'`, tok: epl.STRING, lit: "foo\\bar"},
		{s: `'test`, tok: epl.BADSTRING, lit: `test`},
		{s: "'test\nfoo", tok: epl.BADSTRING, lit: `test`},
		{s: `'test\g'`, tok: epl.BADESCAPE, lit: `\g`, pos: epl.Pos{Line: 0, Char: 6}},

		// Numbers
		{s: `100`, tok: epl.INTEGER, lit: `100`},
		{s: `-100`, tok: epl.INTEGER, lit: `-100`},
		{s: `100.23`, tok: epl.NUMBER, lit: `100.23`},
		{s: `+100.23`, tok: epl.NUMBER, lit: `+100.23`},
		{s: `-100.23`, tok: epl.NUMBER, lit: `-100.23`},
		{s: `-100.`, tok: epl.NUMBER, lit: `-100`},
		{s: `.23`, tok: epl.NUMBER, lit: `.23`},
		{s: `+.23`, tok: epl.NUMBER, lit: `+.23`},
		{s: `-.23`, tok: epl.NUMBER, lit: `-.23`},
		//{s: `.`, tok: epl.ILLEGAL, lit: `.`},
		{s: `-.`, tok: epl.SUB, lit: ``},
		{s: `+.`, tok: epl.ADD, lit: ``},
		{s: `10.3s`, tok: epl.NUMBER, lit: `10.3`},

		// Keywords
		{s: `ALL`, tok: epl.ALL},
		{s: `FROM`, tok: epl.FROM},
		{s: `NI`, tok: epl.NI},
		{s: `IN`, tok: epl.IN},
		{s: `SELECT`, tok: epl.SELECT},
		{s: `WHERE`, tok: epl.WHERE},
	}

	for i, tt := range tests {
		s := epl.NewScanner(strings.NewReader(tt.s))
		tok, pos, lit := s.Scan()
		if tt.tok != tok {
			t.Errorf("%d. %q token mismatch: exp=%q got=%q <%q>", i, tt.s, tt.tok, tok, lit)
		} else if tt.pos.Line != pos.Line || tt.pos.Char != pos.Char {
			t.Errorf("%d. %q pos mismatch: exp=%#v got=%#v", i, tt.s, tt.pos, pos)
		} else if tt.lit != lit {
			t.Errorf("%d. %q literal mismatch: exp=%q got=%q", i, tt.s, tt.lit, lit)
		}
	}
}

// Ensure the scanner can scan a series of tokens correctly.
func TestScanner_Scan_Multi(t *testing.T) {
	type result struct {
		tok epl.Token
		pos epl.Pos
		lit string
	}
	exp := []result{
		{tok: epl.SELECT, pos: epl.Pos{Line: 0, Char: 0}, lit: ""},
		{tok: epl.WS, pos: epl.Pos{Line: 0, Char: 6}, lit: " "},
		{tok: epl.IDENT, pos: epl.Pos{Line: 0, Char: 7}, lit: "value"},
		{tok: epl.WS, pos: epl.Pos{Line: 0, Char: 12}, lit: " "},
		{tok: epl.FROM, pos: epl.Pos{Line: 0, Char: 13}, lit: ""},
		{tok: epl.WS, pos: epl.Pos{Line: 0, Char: 17}, lit: " "},
		{tok: epl.IDENT, pos: epl.Pos{Line: 0, Char: 18}, lit: "myseries"},
		{tok: epl.WS, pos: epl.Pos{Line: 0, Char: 26}, lit: " "},
		{tok: epl.WHERE, pos: epl.Pos{Line: 0, Char: 27}, lit: ""},
		{tok: epl.WS, pos: epl.Pos{Line: 0, Char: 32}, lit: " "},
		{tok: epl.IDENT, pos: epl.Pos{Line: 0, Char: 33}, lit: "a"},
		{tok: epl.WS, pos: epl.Pos{Line: 0, Char: 34}, lit: " "},
		{tok: epl.EQ, pos: epl.Pos{Line: 0, Char: 35}, lit: ""},
		{tok: epl.WS, pos: epl.Pos{Line: 0, Char: 36}, lit: " "},
		{tok: epl.STRING, pos: epl.Pos{Line: 0, Char: 36}, lit: "b"},
		{tok: epl.EOF, pos: epl.Pos{Line: 0, Char: 40}, lit: ""},
	}

	// Create a scanner.
	v := `SELECT value from myseries WHERE a = 'b'`
	s := epl.NewScanner(strings.NewReader(v))

	// Continually scan until we reach the end.
	var act []result
	for {
		tok, pos, lit := s.Scan()
		act = append(act, result{tok, pos, lit})
		if tok == epl.EOF {
			break
		}
	}

	// Verify the token counts match.
	if len(exp) != len(act) {
		t.Fatalf("token count mismatch: exp=%d, got=%d", len(exp), len(act))
	}

	// Verify each token matches.
	for i := range exp {
		if !reflect.DeepEqual(exp[i], act[i]) {
			t.Fatalf("%d. token mismatch:\n\nexp=%#v\n\ngot=%#v", i, exp[i], act[i])
		}
	}
}

// Ensure the library can correctly scan strings.
func TestScanString(t *testing.T) {
	var tests = []struct {
		in  string
		out string
		err string
	}{
		{in: `""`, out: ``},
		{in: `"foo bar"`, out: `foo bar`},
		{in: `'foo bar'`, out: `foo bar`},
		{in: `"foo\nbar"`, out: "foo\nbar"},
		{in: `"foo\\bar"`, out: `foo\bar`},
		{in: `"foo\"bar"`, out: `foo"bar`},
		{in: `'foo\'bar'`, out: `foo'bar`},

		{in: `"foo` + "\n", out: `foo`, err: "bad string"}, // newline in string
		{in: `"foo`, out: `foo`, err: "bad string"},        // unclosed quotes
		{in: `"foo\xbar"`, out: `\x`, err: "bad escape"},   // invalid escape
	}

	for i, tt := range tests {
		out, err := epl.ScanString(strings.NewReader(tt.in))
		if tt.err != errString(err) {
			t.Errorf("%d. %s: error: exp=%s, got=%s", i, tt.in, tt.err, err)
		} else if tt.out != out {
			t.Errorf("%d. %s: out: exp=%s, got=%s", i, tt.in, tt.out, out)
		}
	}
}

// Test scanning regex
func TestScanRegex(t *testing.T) {
	var tests = []struct {
		in  string
		tok epl.Token
		lit string
		err string
	}{
		{in: `/^payments\./`, tok: epl.REGEX, lit: `^payments\.`},
		{in: `/foo\/bar/`, tok: epl.REGEX, lit: `foo/bar`},
		{in: `/foo\\/bar/`, tok: epl.REGEX, lit: `foo\/bar`},
		{in: `/foo\\bar/`, tok: epl.REGEX, lit: `foo\\bar`},
		{in: `/http\:\/\/www\.example\.com/`, tok: epl.REGEX, lit: `http\://www\.example\.com`},
	}

	for i, tt := range tests {
		s := epl.NewScanner(strings.NewReader(tt.in))
		tok, _, lit := s.ScanRegex()
		if tok != tt.tok {
			t.Errorf("%d. %s: error:\n\texp=%s\n\tgot=%s\n", i, tt.in, tt.tok.String(), tok.String())
		}
		if lit != tt.lit {
			t.Errorf("%d. %s: error:\n\texp=%s\n\tgot=%s\n", i, tt.in, tt.lit, lit)
		}
	}
}
