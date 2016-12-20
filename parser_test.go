package jepl_test

import (
	"encoding/json"
	"fmt"
	"github.com/chenyoufu/jepl"
	"reflect"
	"regexp"
	"strings"
	"testing"
)

func TestParseGroupBy(t *testing.T) {
	// For use in various tests.
	var tests = []struct {
		s   string
		d   string
		err string
	}{
		{s: `SELECT sum(x) FROM Packetbeat where uid="xxx" group by tcp.src_ip`, d: `tcp.src_ip`, err: ``},
		{s: `SELECT sum(x) FROM Packetbeat group by tcp.src_ip, tcp.dst_ip`, d: `tcp.src_ip, tcp.dst_ip`, err: ``},
	}
	for i, tt := range tests {
		p := jepl.NewParser(strings.NewReader(tt.s))
		stmt, err := p.ParseStatement()

		if !reflect.DeepEqual(tt.err, errstring(err)) {
			t.Errorf("%d. %q: error mismatch:\n  exp=%s\n  got=%s\n\n", i, tt.s, tt.err, err)
		}

		d := stmt.(*jepl.SelectStatement).Dimensions.String()

		if d != tt.d {
			t.Errorf("%d. %q: error mismatch:\n  exp=%s\n  got=%s\n\n", i, tt.s, tt.d, d)
		}
	}
}

// Ensure the parser can only parse select statement
func TestParseStatement(t *testing.T) {
	// For use in various tests.
	var tests = []struct {
		s    string
		stmt jepl.Statement
		err  string
	}{
		// Errors
		{s: ``, err: `found EOF, expected SELECT at line 1, char 1`},
		{s: `CREATE`, err: `found CREATE, expected SELECT at line 1, char 1`},
		{s: `SELECT sum(x) FROM Packetbeat`, err: ``},
	}
	for i, tt := range tests {
		p := jepl.NewParser(strings.NewReader(tt.s))
		_, err := p.ParseStatement()

		if !reflect.DeepEqual(tt.err, errstring(err)) {
			t.Errorf("%d. %q: error mismatch:\n  exp=%s\n  got=%s\n\n", i, tt.s, tt.err, err)
		}
	}
}

// Ensure the parser can parse strings into Statement ASTs.
func TestParseSelectStatement(t *testing.T) {
	// For use in various tests.
	var tests = []struct {
		s   string
		err string
	}{
		// Errors
		{s: `SELECT`, err: `found EOF, expected identifier, string, number, bool at line 1, char 8`},
		{s: `select 7 from foo`, err: `invalid field 7 in SELECT field, at least one function`},
		{s: `SELECT count(max(value)) FROM myseries`, err: `expected only field argument in count()`},
		{s: `SELECT count(7 * in_bytes) FROM myseries`, err: `expected only field argument in count()`},
		{s: `SELECT count(value), value FROM foo`, err: `invalid field value in SELECT field, at least one function`},
		{s: `select count() from myseries`, err: `invalid number of arguments for count, expected 1, got 0`},

		{s: `SELECT value = 2 FROM cpu`, err: `invalid operator = in SELECT field, only support +-*/`},
		{s: `SELECT s =~ /foo/ FROM cpu`, err: `invalid operator =~ in SELECT field, only support +-*/`},
		{s: `SELECT count(foo + sum(bar)) FROM cpu`, err: `expected only field argument in count()`},
		{s: `SELECT (count(foo + sum(bar))) FROM cpu`, err: `expected only field argument in count()`},
		{s: `SELECT sum(value) + count(foo + sum(bar)) FROM cpu`, err: `binary expressions cannot mix aggregates and raw fields`},

		// Correct
		{s: `SELECT count(x) from foo`, err: ``},
		{s: `SELECT sum(x) from foo`, err: ``},
		{s: `SELECT avg(x) from foo`, err: ``},
		{s: `SELECT count(x), sum(x) from foo`, err: ``},
		{s: `SELECT count(x), sum(x)+sum(y) from foo`, err: ``},
		{s: `SELECT sum(x + y *6 /z) from foo`, err: ``},
		{s: `SELECT sum(x) * (sum(y) / sum(z)) from foo group by host`, err: ``},
	}
	for i, tt := range tests {

		p := jepl.NewParser(strings.NewReader(tt.s))
		_, err := p.ParseStatement()

		if !reflect.DeepEqual(tt.err, errstring(err)) {
			t.Errorf("%d. %q: error mismatch:\n  exp=%s\n  got=%s\n\n", i, tt.s, tt.err, err)
		}
	}
}

// Ensure the parser can parse expressions into an AST.
func TestParser_ParseExpr(t *testing.T) {
	var tests = []struct {
		s    string
		expr jepl.Expr
		err  string
	}{
		// Primitives
		{s: `100.0`, expr: &jepl.NumberLiteral{Val: 100}},
		{s: `100`, expr: &jepl.IntegerLiteral{Val: 100}},
		{s: `'foo bar'`, expr: &jepl.StringLiteral{Val: "foo bar"}},
		{s: `true`, expr: &jepl.BooleanLiteral{Val: true}},
		{s: `false`, expr: &jepl.BooleanLiteral{Val: false}},
		{s: `my_ident`, expr: &jepl.VarRef{Val: "my_ident", Segments: []string{"my_ident"}}},
		// Simple binary expression
		{
			s: `1 + 2`,
			expr: &jepl.BinaryExpr{
				Op:  jepl.ADD,
				LHS: &jepl.IntegerLiteral{Val: 1},
				RHS: &jepl.IntegerLiteral{Val: 2},
			},
		},

		// Binary expression with LHS precedence
		{
			s: `1 * 2 + 3`,
			expr: &jepl.BinaryExpr{
				Op: jepl.ADD,
				LHS: &jepl.BinaryExpr{
					Op:  jepl.MUL,
					LHS: &jepl.IntegerLiteral{Val: 1},
					RHS: &jepl.IntegerLiteral{Val: 2},
				},
				RHS: &jepl.IntegerLiteral{Val: 3},
			},
		},

		// Binary expression with RHS precedence
		{
			s: `1 + 2 * 3`,
			expr: &jepl.BinaryExpr{
				Op:  jepl.ADD,
				LHS: &jepl.IntegerLiteral{Val: 1},
				RHS: &jepl.BinaryExpr{
					Op:  jepl.MUL,
					LHS: &jepl.IntegerLiteral{Val: 2},
					RHS: &jepl.IntegerLiteral{Val: 3},
				},
			},
		},

		// Binary expression with LHS paren group.
		{
			s: `(1 + 2) * 3`,
			expr: &jepl.BinaryExpr{
				Op: jepl.MUL,
				LHS: &jepl.ParenExpr{
					Expr: &jepl.BinaryExpr{
						Op:  jepl.ADD,
						LHS: &jepl.IntegerLiteral{Val: 1},
						RHS: &jepl.IntegerLiteral{Val: 2},
					},
				},
				RHS: &jepl.IntegerLiteral{Val: 3},
			},
		},

		// Binary expression with no precedence, tests left associativity.
		{
			s: `1 * 2 * 3`,
			expr: &jepl.BinaryExpr{
				Op: jepl.MUL,
				LHS: &jepl.BinaryExpr{
					Op:  jepl.MUL,
					LHS: &jepl.IntegerLiteral{Val: 1},
					RHS: &jepl.IntegerLiteral{Val: 2},
				},
				RHS: &jepl.IntegerLiteral{Val: 3},
			},
		},

		// Binary expression with regex.
		{
			s: `region =~ /us.*/`,
			expr: &jepl.BinaryExpr{
				Op:  jepl.EQREGEX,
				LHS: &jepl.VarRef{Val: "region", Segments: []string{"region"}},
				RHS: &jepl.RegexLiteral{Val: regexp.MustCompile(`us.*`)},
			},
		},

		// Binary expression with quoted '/' regex.
		{
			s: `url =~ /http\:\/\/www\.example\.com/`,
			expr: &jepl.BinaryExpr{
				Op:  jepl.EQREGEX,
				LHS: &jepl.VarRef{Val: "url", Segments: []string{"url"}},
				RHS: &jepl.RegexLiteral{Val: regexp.MustCompile(`http\://www\.example\.com`)},
			},
		},

		// Complex binary expression.
		{
			s: `value + 3 < 30 AND 1 + 2 OR true`,
			expr: &jepl.BinaryExpr{
				Op: jepl.OR,
				LHS: &jepl.BinaryExpr{
					Op: jepl.AND,
					LHS: &jepl.BinaryExpr{
						Op: jepl.LT,
						LHS: &jepl.BinaryExpr{
							Op:  jepl.ADD,
							LHS: &jepl.VarRef{Val: "value", Segments: []string{"value"}},
							RHS: &jepl.IntegerLiteral{Val: 3},
						},
						RHS: &jepl.IntegerLiteral{Val: 30},
					},
					RHS: &jepl.BinaryExpr{
						Op:  jepl.ADD,
						LHS: &jepl.IntegerLiteral{Val: 1},
						RHS: &jepl.IntegerLiteral{Val: 2},
					},
				},
				RHS: &jepl.BooleanLiteral{Val: true},
			},
		},

		// Function call (empty)
		{
			s: `my_func()`,
			expr: &jepl.Call{
				Name: "my_func", First: true,
			},
		},

		// Function call (multi-arg)
		{
			s: `my_func(1, 2 + 3)`,
			expr: &jepl.Call{
				Name:  "my_func",
				First: true,
				Args: []jepl.Expr{
					&jepl.IntegerLiteral{Val: 1},
					&jepl.BinaryExpr{
						Op:  jepl.ADD,
						LHS: &jepl.IntegerLiteral{Val: 2},
						RHS: &jepl.IntegerLiteral{Val: 3},
					},
				},
			},
		},
	}

	for i, tt := range tests {
		expr, err := jepl.NewParser(strings.NewReader(tt.s)).ParseExpr()
		if !reflect.DeepEqual(tt.err, errstring(err)) {
			t.Errorf("%d. %q: error mismatch:\n  exp=%s\n  got=%s\n\n", i, tt.s, tt.err, err)
		} else if tt.err == "" && !reflect.DeepEqual(tt.expr, expr) {
			t.Errorf("%d. %q\n\nexpr mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.s, tt.expr, expr)
		}
	}
}

// Ensure a string can be quoted.
func TestQuote(t *testing.T) {
	for i, tt := range []struct {
		in  string
		out string
	}{
		{``, `''`},
		{`foo`, `'foo'`},
		{"foo\nbar", `'foo\nbar'`},
		{`foo bar\\`, `'foo bar\\\\'`},
		{`'foo'`, `'\'foo\''`},
	} {
		if out := jepl.QuoteString(tt.in); tt.out != out {
			t.Errorf("%d. %s: mismatch: %s != %s", i, tt.in, tt.out, out)
		}
	}
}

// Ensure an identifier's segments can be quoted.
func TestQuoteIdent(t *testing.T) {
	for i, tt := range []struct {
		ident []string
		s     string
	}{
		{[]string{``}, ``},
		{[]string{`select`}, `"select"`},
		{[]string{`in-bytes`}, `"in-bytes"`},
		{[]string{`foo`, `bar`}, `"foo".bar`},
		{[]string{`foo`, ``, `bar`}, `"foo"..bar`},
		{[]string{`foo bar`, `baz`}, `"foo bar".baz`},
		{[]string{`foo.bar`, `baz`}, `"foo.bar".baz`},
		{[]string{`foo.bar`, `rp`, `baz`}, `"foo.bar"."rp".baz`},
		{[]string{`foo.bar`, `rp`, `1baz`}, `"foo.bar"."rp"."1baz"`},
	} {
		if s := jepl.QuoteIdent(tt.ident...); tt.s != s {
			t.Errorf("%d. %s: mismatch: %s != %s", i, tt.ident, tt.s, s)
		}
	}
}

// MustParseSelectStatement parses a select statement. Panic on error.
func MustParseSelectStatement(s string) *jepl.SelectStatement {
	stmt, err := jepl.NewParser(strings.NewReader(s)).ParseStatement()
	if err != nil {
		panic(err)
	}
	return stmt.(*jepl.SelectStatement)
}

// MustParseExpr parses an expression. Panic on error.
func MustParseExpr(s string) jepl.Expr {
	expr, err := jepl.NewParser(strings.NewReader(s)).ParseExpr()
	if err != nil {
		fmt.Println(s)
		panic(err)
	}
	return expr
}

// errstring converts an error to its string representation.
func errstring(err error) string {
	if err != nil {
		return err.Error()
	}
	return ""
}

// mustMarshalJSON encodes a value to JSON.
func mustMarshalJSON(v interface{}) []byte {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		panic(err)
	}
	return b
}

func intptr(v int) *int {
	return &v
}

func BenchmarkParseStatement1(b *testing.B) {
	b.ReportAllocs()
	s := `SELECT count(field) FROM series WHERE value > 10`
	for i := 0; i < b.N; i++ {

		if stmt, err := jepl.NewParser(strings.NewReader(s)).ParseStatement(); err != nil {
			b.Fatalf("unexpected error: %s", err)
		} else if stmt == nil {
			b.Fatalf("expected statement: %s", stmt)
		} else {
			_ = stmt.String()
		}
	}
	//	b.SetBytes(int64(len(s)))
}

func BenchmarkParseStatement2(b *testing.B) {
	b.ReportAllocs()
	s := "select max(tcp.in_pkts) from packetbeat where guid = 'for a test you know'"
	for i := 0; i < b.N; i++ {
		if stmt, err := jepl.NewParser(strings.NewReader(s)).ParseStatement(); err != nil {
			b.Fatalf("unexpected error: %s", err)
		} else if stmt == nil {
			b.Fatalf("expected statement: %s", stmt)
		}
	}
	b.SetBytes(int64(len(s)))
}
