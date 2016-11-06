package epl_test

import (
	"epl"
	"reflect"
	"strings"
	"testing"
)

func BenchmarkQuery_String(b *testing.B) {
	p := epl.NewParser(strings.NewReader(`SELECT foo AS zoo, a AS b FROM bar WHERE value > 10 AND q = 'hello'`))
	q, _ := p.ParseStatement()
	for i := 0; i < b.N; i++ {
		_ = q.String()
	}
}

// Ensure a value's data type can be retrieved.
func TestInspectDataType(t *testing.T) {
	for i, tt := range []struct {
		v   interface{}
		typ epl.DataType
	}{
		{float64(100), epl.Float},
		{int64(100), epl.Integer},
		{int32(100), epl.Integer},
		{100, epl.Integer},
		{true, epl.Boolean},
		{"string", epl.String},
		{nil, epl.Unknown},
	} {
		if typ := epl.InspectDataType(tt.v); tt.typ != typ {
			t.Errorf("%d. %v (%s): unexpected type: %s", i, tt.v, tt.typ, typ)
			continue
		}
	}
}

func TestDataType_String(t *testing.T) {
	for i, tt := range []struct {
		typ epl.DataType
		v   string
	}{
		{epl.Float, "float"},
		{epl.Integer, "integer"},
		{epl.Boolean, "boolean"},
		{epl.String, "string"},
		{epl.Unknown, "unknown"},
	} {
		if v := tt.typ.String(); tt.v != v {
			t.Errorf("%d. %v (%s): unexpected string: %s", i, tt.typ, tt.v, v)
		}
	}
}

// Ensure the idents from the select clause can come out
func TestSelect_NamesInSelect(t *testing.T) {
	s := MustParseSelectStatement("select count(asdf), count(bar) from cpu")
	a := s.NamesInSelect()
	if !reflect.DeepEqual(a, []string{"asdf", "bar"}) {
		t.Fatal("expected names asdf and bar")
	}
}

// Ensure the idents from the where clause can come out
func TestSelect_NamesInWhere(t *testing.T) {
	s := MustParseSelectStatement("select sum(xxx) from cpu where time > 23 AND (asdf = 'jkl' OR (foo = 'bar' AND baz = 'bar'))")
	a := s.NamesInWhere()
	if !reflect.DeepEqual(a, []string{"time", "asdf", "foo", "baz"}) {
		t.Fatalf("exp: time,asdf,foo,baz\ngot: %s\n", strings.Join(a, ","))
	}
}

// Ensure that the IsRawQuery flag gets set properly
func TestSelectStatement_IsRawQuerySet(t *testing.T) {
	var tests = []struct {
		stmt  string
		isRaw bool
	}{
		{
			stmt:  "select value1,value2 from foo",
			isRaw: true,
		},
		{
			stmt:  "select value1,value2 from foo, time(10m)",
			isRaw: true,
		},
		{
			stmt:  "select mean(value) from foo group by bar",
			isRaw: false,
		},
		{
			stmt:  "select mean(value) from foo group by *",
			isRaw: false,
		},
		{
			stmt:  "select mean(value) from foo group by *",
			isRaw: false,
		},
	}

	for _, tt := range tests {
		s := MustParseSelectStatement(tt.stmt)
		if s.IsRawQuery != tt.isRaw {
			t.Errorf("'%s', IsRawQuery should be %v", tt.stmt, tt.isRaw)
		}
	}
}

// Ensure binary expression names can be evaluated.
func TestBinaryExprName(t *testing.T) {
	for i, tt := range []struct {
		expr string
		name string
	}{
		{expr: `value + 1`, name: `value`},
		{expr: `"user" / total`, name: `user_total`},
		{expr: `("user" + total) / total`, name: `user_total_total`},
	} {
		expr, _ := epl.ParseExpr(tt.expr)
		switch expr := expr.(type) {
		case *epl.BinaryExpr:
			name := epl.BinaryExprName(expr)
			if name != tt.name {
				t.Errorf("%d. unexpected name %s, got %s", i, name, tt.name)
			}
		default:
			t.Errorf("%d. unexpected expr type: %T", i, expr)
		}
	}
}

// Ensure that we see if a where clause has only time limitations
func TestOnlyTimeExpr(t *testing.T) {
	var tests = []struct {
		stmt string
		exp  bool
	}{
		{
			stmt: `SELECT value FROM myseries WHERE value > 1`,
			exp:  false,
		},
		{
			stmt: `SELECT value FROM foo WHERE time >= '2000-01-01T00:00:05Z'`,
			exp:  true,
		},
		{
			stmt: `SELECT value FROM foo WHERE time >= '2000-01-01T00:00:05Z' AND time < '2000-01-01T00:00:05Z'`,
			exp:  true,
		},
		{
			stmt: `SELECT value FROM foo WHERE time >= '2000-01-01T00:00:05Z' AND asdf = 'bar'`,
			exp:  false,
		},
		{
			stmt: `SELECT value FROM foo WHERE asdf = 'jkl' AND (time >= '2000-01-01T00:00:05Z' AND time < '2000-01-01T00:00:05Z')`,
			exp:  false,
		},
	}

	for i, tt := range tests {
		// Parse statement.
		stmt, err := epl.NewParser(strings.NewReader(tt.stmt)).ParseStatement()
		if err != nil {
			t.Fatalf("invalid statement: %q: %s", tt.stmt, err)
		}
		if epl.OnlyTimeExpr(stmt.(*epl.SelectStatement).Condition) != tt.exp {
			t.Fatalf("%d. expected statement to return only time dimension to be %t: %s", i, tt.exp, tt.stmt)
		}
	}
}

// Ensure an expression can be reduced.
func TestEval(t *testing.T) {
	for i, tt := range []struct {
		in   string
		out  interface{}
		data map[string]interface{}
	}{
		// Number literals.
		// {in: `1 + 2`, out: int64(3)},
		{in: `(foo*2) + ( (4/2) + (3 * 5) - 0.5 )`, out: float64(26.5), data: map[string]interface{}{"foo": float64(5)}},
		{in: `foo / 2`, out: float64(2), data: map[string]interface{}{"foo": float64(4)}},
		{in: `4 = 4`, out: true},
		{in: `4 <> 4`, out: false},
		{in: `6 > 4`, out: true},
		{in: `4 >= 4`, out: true},
		{in: `4 < 6`, out: true},
		{in: `4 <= 4`, out: true},
		{in: `4 AND 5`, out: nil},
		{in: `0 = 'test'`, out: false},
		{in: `1.0 = 1`, out: true},
		{in: `1.2 = 1`, out: false},

		// Boolean literals.
		{in: `true AND false`, out: false},
		{in: `true OR false`, out: true},
		{in: `false = 4`, out: false},

		// String literals.
		{in: `'foo' = 'bar'`, out: false},
		{in: `'foo' = 'foo'`, out: true},
		{in: `'' = 4`, out: false},

		// Regex literals.
		{in: `'foo' =~ /f.*/`, out: true},
		{in: `'foo' =~ /b.*/`, out: false},
		{in: `'foo' !~ /f.*/`, out: false},
		{in: `'foo' !~ /b.*/`, out: true},

		// Variable references.
		{in: `foo`, out: "bar", data: map[string]interface{}{"foo": "bar"}},
		{in: `foo = 'bar'`, out: true, data: map[string]interface{}{"foo": "bar"}},
		{in: `foo = 'bar'`, out: nil, data: map[string]interface{}{"foo": nil}},
		{in: `foo <> 'bar'`, out: true, data: map[string]interface{}{"foo": "xxx"}},
		{in: `foo =~ /b.*/`, out: true, data: map[string]interface{}{"foo": "bar"}},
		{in: `foo !~ /b.*/`, out: false, data: map[string]interface{}{"foo": "bar"}},
	} {
		// Evaluate expression.
		out := epl.Eval(MustParseExpr(tt.in), tt.data)

		// Compare with expected output.
		if !reflect.DeepEqual(tt.out, out) {
			t.Errorf("%d. %s: unexpected output:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.in, tt.out, out)
			continue
		}
	}
}

func Test_fieldsNames(t *testing.T) {
	for _, test := range []struct {
		in    []string
		out   []string
		alias []string
	}{
		{ //case: binary expr(valRef)
			in:    []string{"value+value"},
			out:   []string{"value", "value"},
			alias: []string{"value_value"},
		},
		{ //case: binary expr + valRef
			in:    []string{"value+value", "temperature"},
			out:   []string{"value", "value", "temperature"},
			alias: []string{"value_value", "temperature"},
		},
		{ //case: aggregate expr
			in:    []string{"mean(value)"},
			out:   []string{"mean"},
			alias: []string{"mean"},
		},
		{ //case: binary expr(aggregate expr)
			in:    []string{"mean(value) + max(value)"},
			out:   []string{"value", "value"},
			alias: []string{"mean_max"},
		},
		{ //case: binary expr(aggregate expr) + valRef
			in:    []string{"mean(value) + max(value)", "temperature"},
			out:   []string{"value", "value", "temperature"},
			alias: []string{"mean_max", "temperature"},
		},
		{ //case: mixed aggregate and varRef
			in:    []string{"mean(value) + temperature"},
			out:   []string{"value", "temperature"},
			alias: []string{"mean_temperature"},
		},
		{ //case: ParenExpr(varRef)
			in:    []string{"(value)"},
			out:   []string{"value"},
			alias: []string{"value"},
		},
		{ //case: ParenExpr(varRef + varRef)
			in:    []string{"(value + value)"},
			out:   []string{"value", "value"},
			alias: []string{"value_value"},
		},
		{ //case: ParenExpr(aggregate)
			in:    []string{"(mean(value))"},
			out:   []string{"value"},
			alias: []string{"mean"},
		},
		{ //case: ParenExpr(aggregate + aggregate)
			in:    []string{"(mean(value) + max(value))"},
			out:   []string{"value", "value"},
			alias: []string{"mean_max"},
		},
	} {
		fields := epl.Fields{}
		for _, s := range test.in {
			expr := MustParseExpr(s)
			fields = append(fields, &epl.Field{Expr: expr})
		}
		got := fields.Names()
		if !reflect.DeepEqual(got, test.out) {
			t.Errorf("get fields name:\nexp=%v\ngot=%v\n", test.out, got)
		}
		alias := fields.AliasNames()
		if !reflect.DeepEqual(alias, test.alias) {
			t.Errorf("get fields alias name:\nexp=%v\ngot=%v\n", test.alias, alias)
		}
	}

}

func TestSelect_ColumnNames(t *testing.T) {
	for i, tt := range []struct {
		stmt    *epl.SelectStatement
		columns []string
	}{
		{
			stmt: &epl.SelectStatement{
				Fields: epl.Fields([]*epl.Field{
					{Expr: &epl.VarRef{Val: "value"}},
				}),
			},
			columns: []string{"value"},
		},
		{
			stmt: &epl.SelectStatement{
				Fields: epl.Fields([]*epl.Field{
					{Expr: &epl.Call{Name: "sum"}},
				}),
			},
			columns: []string{"sum"},
		},
		{
			stmt: &epl.SelectStatement{
				Fields: epl.Fields([]*epl.Field{
					{Expr: &epl.VarRef{Val: "value"}},
					{Expr: &epl.VarRef{Val: "value"}},
					{Expr: &epl.VarRef{Val: "value_1"}},
				}),
			},
			columns: []string{"value", "value_1", "value_1_1"},
		},
		{
			stmt: &epl.SelectStatement{
				Fields: epl.Fields([]*epl.Field{
					{Expr: &epl.VarRef{Val: "value"}},
					{Expr: &epl.VarRef{Val: "value_1"}},
					{Expr: &epl.VarRef{Val: "value"}},
				}),
			},
			columns: []string{"value", "value_1", "value_2"},
		},
		{
			stmt: &epl.SelectStatement{
				Fields: epl.Fields([]*epl.Field{
					{Expr: &epl.VarRef{Val: "value"}},
					{Expr: &epl.VarRef{Val: "total"}, Alias: "value"},
					{Expr: &epl.VarRef{Val: "value"}},
				}),
			},
			columns: []string{"value_1", "value", "value_2"},
		},
	} {
		columns := tt.stmt.ColumnNames()
		if !reflect.DeepEqual(columns, tt.columns) {
			t.Errorf("%d. expected %s, got %s", i, tt.columns, columns)
		}
	}
}

func TestSources_Names(t *testing.T) {
	sources := epl.Sources([]epl.Source{
		&epl.Measurement{
			Name: "cpu",
		},
		&epl.Measurement{
			Name: "mem",
		},
	})

	names := sources.Names()
	if names[0] != "cpu" {
		t.Errorf("expected cpu, got %s", names[0])
	}
	if names[1] != "mem" {
		t.Errorf("expected mem, got %s", names[1])
	}
}

// Valuer represents a simple wrapper around a map to implement the epl.Valuer interface.
type Valuer map[string]interface{}

// Value returns the value and existence of a key.
func (o Valuer) Value(key string) (v interface{}, ok bool) {
	v, ok = o[key]
	return
}
