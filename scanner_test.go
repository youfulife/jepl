package jepl_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/chenyoufu/jepl"
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
		tok jepl.Token
		lit string
		pos jepl.Pos
	}{
		// Special tokens (EOF, ILLEGAL, WS)
		{s: ``, tok: jepl.EOF},
		{s: `#`, tok: jepl.ILLEGAL, lit: `#`},
		{s: ` `, tok: jepl.WS, lit: " "},
		{s: "\t", tok: jepl.WS, lit: "\t"},
		{s: "\n", tok: jepl.WS, lit: "\n"},
		{s: "\r", tok: jepl.WS, lit: "\n"},
		{s: "\r\n", tok: jepl.WS, lit: "\n"},
		{s: "\rX", tok: jepl.WS, lit: "\n"},
		{s: "\n\r", tok: jepl.WS, lit: "\n\n"},
		{s: " \n\t \r\n\t", tok: jepl.WS, lit: " \n\t \n\t"},
		{s: " foo", tok: jepl.WS, lit: " "},

		// Numeric operators
		{s: `+`, tok: jepl.ADD},
		{s: `-`, tok: jepl.SUB},
		{s: `*`, tok: jepl.MUL},
		{s: `/`, tok: jepl.DIV},
		{s: `%`, tok: jepl.MOD},

		// Logical operators
		{s: `AND`, tok: jepl.AND},
		{s: `and`, tok: jepl.AND},
		{s: `OR`, tok: jepl.OR},
		{s: `or`, tok: jepl.OR},
		{s: `NI`, tok: jepl.NI},
		{s: `IN`, tok: jepl.IN},

		{s: `=`, tok: jepl.EQ},
		{s: `!=`, tok: jepl.NEQ},
		{s: `! `, tok: jepl.ILLEGAL, lit: "!"},
		{s: `<`, tok: jepl.LT},
		{s: `<=`, tok: jepl.LTE},
		{s: `>`, tok: jepl.GT},
		{s: `>=`, tok: jepl.GTE},

		// Misc tokens
		{s: `[`, tok: jepl.LBRACKET},
		{s: `(`, tok: jepl.LPAREN},
		{s: `]`, tok: jepl.RBRACKET},
		{s: `)`, tok: jepl.RPAREN},
		{s: `,`, tok: jepl.COMMA},
		{s: `.`, tok: jepl.DOT},
		{s: `=~`, tok: jepl.EQREGEX},
		{s: `!~`, tok: jepl.NEQREGEX},

		// Identifiers
		{s: `foo`, tok: jepl.IDENT, lit: `foo`},
		{s: `_foo`, tok: jepl.IDENT, lit: `_foo`},
		{s: `Zx12_3U_-`, tok: jepl.IDENT, lit: `Zx12_3U_`},

		{s: `true`, tok: jepl.TRUE},
		{s: `false`, tok: jepl.FALSE},

		// Strings
		{s: `"foo"`, tok: jepl.STRING, lit: `foo`},
		{s: `"foo\\bar"`, tok: jepl.STRING, lit: `foo\bar`},
		{s: `"foo\bar"`, tok: jepl.BADESCAPE, lit: `\b`, pos: jepl.Pos{Line: 0, Char: 5}},
		{s: `"foo\"bar\""`, tok: jepl.STRING, lit: `foo"bar"`},
		{s: `test"`, tok: jepl.BADSTRING, lit: "", pos: jepl.Pos{Line: 0, Char: 3}},
		{s: `"test`, tok: jepl.BADSTRING, lit: `test`},

		{s: `'testing 123!'`, tok: jepl.STRING, lit: `testing 123!`},
		{s: `'foo\nbar'`, tok: jepl.STRING, lit: "foo\nbar"},
		{s: `'foo\\bar'`, tok: jepl.STRING, lit: "foo\\bar"},
		{s: `'test`, tok: jepl.BADSTRING, lit: `test`},
		{s: "'test\nfoo", tok: jepl.BADSTRING, lit: `test`},
		{s: `'test\g'`, tok: jepl.BADESCAPE, lit: `\g`, pos: jepl.Pos{Line: 0, Char: 6}},

		// Numbers
		{s: `100`, tok: jepl.INTEGER, lit: `100`},
		{s: `-100`, tok: jepl.INTEGER, lit: `-100`},
		{s: `100.23`, tok: jepl.NUMBER, lit: `100.23`},
		{s: `+100.23`, tok: jepl.NUMBER, lit: `+100.23`},
		{s: `-100.23`, tok: jepl.NUMBER, lit: `-100.23`},
		{s: `-100.`, tok: jepl.NUMBER, lit: `-100`},
		{s: `.23`, tok: jepl.NUMBER, lit: `.23`},
		{s: `+.23`, tok: jepl.NUMBER, lit: `+.23`},
		{s: `-.23`, tok: jepl.NUMBER, lit: `-.23`},
		//{s: `.`, tok: jepl.ILLEGAL, lit: `.`},
		{s: `-.`, tok: jepl.SUB, lit: ``},
		{s: `+.`, tok: jepl.ADD, lit: ``},
		{s: `10.3s`, tok: jepl.NUMBER, lit: `10.3`},

		// Keywords
		{s: `ALL`, tok: jepl.ALL},
		{s: `FROM`, tok: jepl.FROM},
		{s: `SELECT`, tok: jepl.SELECT},
		{s: `WHERE`, tok: jepl.WHERE},
		{s: `GROUP`, tok: jepl.GROUP},
		{s: `BY`, tok: jepl.BY},
	}

	for i, tt := range tests {
		s := jepl.NewScanner(strings.NewReader(tt.s))
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
		tok jepl.Token
		pos jepl.Pos
		lit string
	}
	exp := []result{
		{tok: jepl.SELECT, pos: jepl.Pos{Line: 0, Char: 0}, lit: ""},
		{tok: jepl.WS, pos: jepl.Pos{Line: 0, Char: 6}, lit: " "},
		{tok: jepl.IDENT, pos: jepl.Pos{Line: 0, Char: 7}, lit: "value"},
		{tok: jepl.WS, pos: jepl.Pos{Line: 0, Char: 12}, lit: " "},
		{tok: jepl.FROM, pos: jepl.Pos{Line: 0, Char: 13}, lit: ""},
		{tok: jepl.WS, pos: jepl.Pos{Line: 0, Char: 17}, lit: " "},
		{tok: jepl.IDENT, pos: jepl.Pos{Line: 0, Char: 18}, lit: "myseries"},
		{tok: jepl.WS, pos: jepl.Pos{Line: 0, Char: 26}, lit: " "},
		{tok: jepl.WHERE, pos: jepl.Pos{Line: 0, Char: 27}, lit: ""},
		{tok: jepl.WS, pos: jepl.Pos{Line: 0, Char: 32}, lit: " "},
		{tok: jepl.IDENT, pos: jepl.Pos{Line: 0, Char: 33}, lit: "a"},
		{tok: jepl.WS, pos: jepl.Pos{Line: 0, Char: 34}, lit: " "},
		{tok: jepl.EQ, pos: jepl.Pos{Line: 0, Char: 35}, lit: ""},
		{tok: jepl.WS, pos: jepl.Pos{Line: 0, Char: 36}, lit: " "},
		{tok: jepl.STRING, pos: jepl.Pos{Line: 0, Char: 36}, lit: "b"},
		{tok: jepl.EOF, pos: jepl.Pos{Line: 0, Char: 40}, lit: ""},
	}

	// Create a scanner.
	v := `SELECT value from myseries WHERE a = 'b'`
	s := jepl.NewScanner(strings.NewReader(v))

	// Continually scan until we reach the end.
	var act []result
	for {
		tok, pos, lit := s.Scan()
		act = append(act, result{tok, pos, lit})
		if tok == jepl.EOF {
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
		out, err := jepl.ScanString(strings.NewReader(tt.in))
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
		tok jepl.Token
		lit string
		err string
	}{
		{in: `/^payments\./`, tok: jepl.REGEX, lit: `^payments\.`},
		{in: `/foo\/bar/`, tok: jepl.REGEX, lit: `foo/bar`},
		{in: `/foo\\/bar/`, tok: jepl.REGEX, lit: `foo\/bar`},
		{in: `/foo\\bar/`, tok: jepl.REGEX, lit: `foo\\bar`},
		{in: `/http\:\/\/www\.example\.com/`, tok: jepl.REGEX, lit: `http\://www\.example\.com`},
	}

	for i, tt := range tests {
		s := jepl.NewScanner(strings.NewReader(tt.in))
		tok, _, lit := s.ScanRegex()
		if tok != tt.tok {
			t.Errorf("%d. %s: error:\n\texp=%s\n\tgot=%s\n", i, tt.in, tt.tok.String(), tok.String())
		}
		if lit != tt.lit {
			t.Errorf("%d. %s: error:\n\texp=%s\n\tgot=%s\n", i, tt.in, tt.lit, lit)
		}
	}
}
