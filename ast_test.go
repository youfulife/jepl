package jepl_test

import (
	"github.com/chenyoufu/jepl"
	"reflect"
	"strings"
	"testing"
)

/*
func BenchmarkQuery_String(b *testing.B) {
	p := jepl.NewParser(strings.NewReader("SELECT foo AS zoo FROM bar WHERE value > 10 AND q = 'hello'"))
	q, err := p.ParseStatement()
	if err != nil {
		b.Error(err)
	}
	for i := 0; i < b.N; i++ {
		_ = q.String()
	}
}
*/

// Ensure a value's data type can be retrieved.
func TestInspectDataType(t *testing.T) {
	for i, tt := range []struct {
		v   interface{}
		typ jepl.DataType
	}{
		{float64(100), jepl.Float},
		{int64(100), jepl.Integer},
		{int32(100), jepl.Integer},
		{100, jepl.Integer},
		{true, jepl.Boolean},
		{"string", jepl.String},
		{nil, jepl.Unknown},
	} {
		if typ := jepl.InspectDataType(tt.v); tt.typ != typ {
			t.Errorf("%d. %v (%s): unexpected type: %s", i, tt.v, tt.typ, typ)
			continue
		}
	}
}

func TestDataType_String(t *testing.T) {
	for i, tt := range []struct {
		typ jepl.DataType
		v   string
	}{
		{jepl.Float, "float"},
		{jepl.Integer, "integer"},
		{jepl.Boolean, "boolean"},
		{jepl.String, "string"},
		{jepl.Unknown, "unknown"},
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

// Ensure an expression can be reduced.

func TestEval(t *testing.T) {
	for _, _ = range []struct {
		in   string
		out  interface{}
		data map[string]interface{}
	}{
		// Number literals.
		{in: `uid IN [1,2,3]`, out: true, data: map[string]interface{}{"uid": 1}},
		{in: `uid IN [1,2,3]`, out: false, data: map[string]interface{}{"uid": 4}},
		{in: `uid NI [1,2,3]`, out: false, data: map[string]interface{}{"uid": 1}},
		{in: `uid NI [1,2,3]`, out: true, data: map[string]interface{}{"uid": 4}},
		{in: `foo IN ['xxx','yyy','zzz']`, out: true, data: map[string]interface{}{"foo": "xxx"}},
		{in: `foo NI ['xxx','yyy','zzz']`, out: true, data: map[string]interface{}{"foo": "uuu"}},
		{in: `1 + 2`, out: int64(3)},
		{in: `(foo*2) + ( (4/2) + (3 * 5) - 0.5 )`, out: float64(26.5), data: map[string]interface{}{"foo": float64(5)}},
		{in: `foo / 2`, out: int64(2), data: map[string]interface{}{"foo": 4}},
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
		/*
		// Evaluate expression.
		out := jepl.Eval(MustParseExpr(tt.in), tt.data)
		// Compare with expected output.
		if !reflect.DeepEqual(tt.out, out) {
			t.Errorf("%d. %s: unexpected output:\nexp=%T, %#v\ngot=%T, %#v\n", i, tt.in, tt.out, tt.out, out, out)

			continue
		}
		*/
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
		fields := jepl.Fields{}
		for _, s := range test.in {
			expr := MustParseExpr(s)
			fields = append(fields, &jepl.Field{Expr: expr})
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
		stmt    *jepl.SelectStatement
		columns []string
	}{
		{
			stmt: &jepl.SelectStatement{
				Fields: jepl.Fields([]*jepl.Field{
					{Expr: &jepl.VarRef{Val: "value"}},
				}),
			},
			columns: []string{"value"},
		},
		{
			stmt: &jepl.SelectStatement{
				Fields: jepl.Fields([]*jepl.Field{
					{Expr: &jepl.Call{Name: "sum"}},
				}),
			},
			columns: []string{"sum"},
		},
		{
			stmt: &jepl.SelectStatement{
				Fields: jepl.Fields([]*jepl.Field{
					{Expr: &jepl.VarRef{Val: "value"}},
					{Expr: &jepl.VarRef{Val: "value"}},
					{Expr: &jepl.VarRef{Val: "value_1"}},
				}),
			},
			columns: []string{"value", "value_1", "value_1_1"},
		},
		{
			stmt: &jepl.SelectStatement{
				Fields: jepl.Fields([]*jepl.Field{
					{Expr: &jepl.VarRef{Val: "value"}},
					{Expr: &jepl.VarRef{Val: "value_1"}},
					{Expr: &jepl.VarRef{Val: "value"}},
				}),
			},
			columns: []string{"value", "value_1", "value_2"},
		},
		{
			stmt: &jepl.SelectStatement{
				Fields: jepl.Fields([]*jepl.Field{
					{Expr: &jepl.VarRef{Val: "value"}},
					{Expr: &jepl.VarRef{Val: "total"}, Alias: "value"},
					{Expr: &jepl.VarRef{Val: "value"}},
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

// Valuer represents a simple wrapper around a map to implement the jepl.Valuer interface.
type Valuer map[string]interface{}

// Value returns the value and existence of a key.
func (o Valuer) Value(key string) (v interface{}, ok bool) {
	v, ok = o[key]
	return
}
