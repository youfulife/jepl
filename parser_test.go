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

// Ensure the parser can parse strings into Statement ASTs.
func TestParser_ParseStatement(t *testing.T) {
	// For use in various tests.
	var tests = []struct {
		skip   bool
		s      string
		params map[string]interface{}
		stmt   jepl.Statement
		err    string
	}{
		// Errors
		{s: ``, err: `found EOF, expected SELECT at line 1, char 1`},
		{s: `SELECT`, err: `found EOF, expected identifier, string, number, bool at line 1, char 8`},
		{s: `SELECT count(max(value)) FROM myseries`, err: `expected field argument in count()`},
		{s: `SELECT count(distinct('value')) FROM myseries`, err: `expected field argument in count()`},
		{s: `SELECT min(max(value)) FROM myseries`, err: `expected field argument in min()`},
		{s: `SELECT min(distinct(value)) FROM myseries`, err: `expected field argument in min()`},
		{s: `SELECT max(max(value)) FROM myseries`, err: `expected field argument in max()`},
		{s: `SELECT sum(max(value)) FROM myseries`, err: `expected field argument in sum()`},
		{s: `SELECT count(value), value FROM foo`, err: `mixing aggregate and non-aggregate queries is not supported`},
		{s: `select count() from myseries`, err: `invalid number of arguments for count, expected 1, got 0`},

		{s: `SELECT value = 2 FROM cpu`, err: `invalid operator = in SELECT clause at line 1, char 8; operator is intended for WHERE clause`},
		{s: `SELECT s =~ /foo/ FROM cpu`, err: `invalid operator =~ in SELECT clause at line 1, char 8; operator is intended for WHERE clause`},
		{s: `SELECT count(foo + sum(bar)) FROM cpu`, err: `expected field argument in count()`},
		{s: `SELECT (count(foo + sum(bar))) FROM cpu`, err: `expected field argument in count()`},
		{s: `SELECT sum(value) + count(foo + sum(bar)) FROM cpu`, err: `binary expressions cannot mix aggregates and raw fields`},
	}
	for i, tt := range tests {
		if tt.skip {
			continue
		}
		p := jepl.NewParser(strings.NewReader(tt.s))
		stmt, err := p.ParseStatement()

		if !reflect.DeepEqual(tt.err, errstring(err)) {
			t.Errorf("%d. %q: error mismatch:\n  exp=%s\n  got=%s\n\n", i, tt.s, tt.err, err)
		} else if tt.err == "" {
			if !reflect.DeepEqual(tt.stmt, stmt) {
				t.Logf("\n# %s\nexp=%s\ngot=%s\n", tt.s, mustMarshalJSON(tt.stmt), mustMarshalJSON(stmt))
				t.Logf("\nSQL exp=%s\nSQL got=%s\n", tt.stmt.String(), stmt.String())
				t.Errorf("%d. %q\n\nstmt mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.s, tt.stmt, stmt)
			} else {
				stmt2, err := jepl.ParseStatement(stmt.String())
				if err != nil {
					t.Errorf("%d. %q: unable to parse statement string: %s", i, stmt.String(), err)
				} else if !reflect.DeepEqual(tt.stmt, stmt2) {
					t.Logf("\n# %s\nexp=%s\ngot=%s\n", tt.s, mustMarshalJSON(tt.stmt), mustMarshalJSON(stmt2))
					t.Logf("\nSQL exp=%s\nSQL got=%s\n", tt.stmt.String(), stmt2.String())
					t.Errorf("%d. %q\n\nstmt reparse mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.s, tt.stmt, stmt2)
				}
			}
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
				Name: "my_func",
			},
		},

		// Function call (multi-arg)
		{
			s: `my_func(1, 2 + 3)`,
			expr: &jepl.Call{
				Name: "my_func",
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

func BenchmarkParserParseStatement(b *testing.B) {
	b.ReportAllocs()
	s := `SELECT "field" FROM "series" WHERE value > 10`
	for i := 0; i < b.N; i++ {
		if stmt, err := jepl.NewParser(strings.NewReader(s)).ParseStatement(); err != nil {
			b.Fatalf("unexpected error: %s", err)
		} else if stmt == nil {
			b.Fatalf("expected statement: %s", stmt)
		}
	}
	b.SetBytes(int64(len(s)))
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
