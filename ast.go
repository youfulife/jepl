package jepl

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/bitly/go-simplejson"
	"reflect"
	"regexp"
	"regexp/syntax"
	"strconv"
	"strings"
	"time"
)

// DataType represents the primitive data types available in InfluxQL.
type DataType int

const (
	// Unknown primitive data type.
	Unknown DataType = 0
	// Float means the data type is a float
	Float = 1
	// Integer means the data type is a integer
	Integer = 2
	// String means the data type is a string of text.
	String = 3
	// Boolean means the data type is a boolean.
	Boolean = 4
	// AnyField means the data type is any field.
	AnyField = 5
)

var (
	// ErrInvalidTime is returned when the timestamp string used to
	// compare against time field is invalid.
	ErrInvalidTime = errors.New("invalid timestamp string")
)

// InspectDataType returns the data type of a given value.
func InspectDataType(v interface{}) DataType {
	switch v.(type) {
	case float64:
		return Float
	case int64, int32, int:
		return Integer
	case string:
		return String
	case bool:
		return Boolean
	default:
		return Unknown
	}
}

// InspectDataTypes returns all of the data types for an interface slice.
func InspectDataTypes(a []interface{}) []DataType {
	dta := make([]DataType, len(a))
	for i, v := range a {
		dta[i] = InspectDataType(v)
	}
	return dta
}

func (d DataType) String() string {
	switch d {
	case Float:
		return "float"
	case Integer:
		return "integer"
	case String:
		return "string"
	case Boolean:
		return "boolean"
	case AnyField:
		return "field"
	}
	return "unknown"
}

// Node represents a node in the InfluxDB abstract syntax tree.
type Node interface {
	node()
	String() string
}

func (Statements) node() {}

func (*SelectStatement) node() {}

func (*BinaryExpr) node()     {}
func (*BooleanLiteral) node() {}
func (*Call) node()           {}
func (*IntegerLiteral) node() {}
func (*Field) node()          {}
func (Fields) node()          {}
func (*Measurement) node()    {}
func (Measurements) node()    {}
func (*nilLiteral) node()     {}
func (*NumberLiteral) node()  {}
func (*ParenExpr) node()      {}
func (*RegexLiteral) node()   {}
func (*ListLiteral) node()    {}
func (Sources) node()         {}
func (*StringLiteral) node()  {}
func (*VarRef) node()         {}
func (*Wildcard) node()       {}

// Statements represents a list of statements.
type Statements []Statement

// String returns a string representation of the statements.
func (a Statements) String() string {
	var str []string
	for _, stmt := range a {
		str = append(str, stmt.String())
	}
	return strings.Join(str, ";\n")
}

// Statement represents a single command in InfluxQL.
type Statement interface {
	Node
	stmt()
}

// HasDefaultDatabase provides an interface to get the default database from a Statement.
type HasDefaultDatabase interface {
	Node
	stmt()
	DefaultDatabase() string
}

func (*SelectStatement) stmt() {}

// Expr represents an expression that can be evaluated to a value.
type Expr interface {
	Node
	expr()
}

func (*BinaryExpr) expr()     {}
func (*BooleanLiteral) expr() {}
func (*Call) expr()           {}
func (*IntegerLiteral) expr() {}
func (*nilLiteral) expr()     {}
func (*NumberLiteral) expr()  {}
func (*ParenExpr) expr()      {}
func (*RegexLiteral) expr()   {}
func (*ListLiteral) expr()    {}
func (*StringLiteral) expr()  {}
func (*VarRef) expr()         {}
func (*Wildcard) expr()       {}

// Literal represents a static literal.
type Literal interface {
	Expr
	literal()
}

func (*BooleanLiteral) literal() {}
func (*IntegerLiteral) literal() {}
func (*nilLiteral) literal()     {}
func (*NumberLiteral) literal()  {}
func (*RegexLiteral) literal()   {}
func (*ListLiteral) literal()    {}
func (*StringLiteral) literal()  {}

// Source represents a source of data for a statement.
type Source interface {
	Node
	source()
}

func (*Measurement) source() {}

// Sources represents a list of sources.
type Sources []Source

// Names returns a list of source names.
func (a Sources) Names() []string {
	names := make([]string, 0, len(a))
	for _, s := range a {
		switch s := s.(type) {
		case *Measurement:
			names = append(names, s.Database)
		}
	}
	return names
}

// String returns a string representation of a Sources array.
func (a Sources) String() string {
	var buf bytes.Buffer

	ubound := len(a) - 1
	for i, src := range a {
		_, _ = buf.WriteString(src.String())
		if i < ubound {
			_, _ = buf.WriteString(", ")
		}
	}

	return buf.String()
}

// SelectStatement represents a command for extracting data from the database.
type SelectStatement struct {
	// Expressions returned from the selection.
	Fields Fields

	// Data sources that fields are extracted from.
	Sources Sources

	// An expression evaluated on data point.
	Condition Expr

	// if it's a query for raw data values (i.e. not an aggregate)
	IsRawQuery bool

	// Removes duplicate rows from raw queries.
	Dedupe bool
}

// matchExactRegex matches regexes that have the following form: /^foo$/. It
// considers /^$/ to be a matching regex.
func matchExactRegex(v string) (string, bool) {
	re, err := syntax.Parse(v, syntax.Perl)
	if err != nil {
		// Nothing we can do or log.
		return "", false
	}

	if re.Op != syntax.OpConcat {
		return "", false
	}

	if len(re.Sub) < 2 || len(re.Sub) > 3 {
		// Regex has too few or too many subexpressions.
		return "", false
	}

	start := re.Sub[0]
	if !(start.Op == syntax.OpBeginLine || start.Op == syntax.OpBeginText) {
		// Regex does not begin with ^
		return "", false
	}

	end := re.Sub[len(re.Sub)-1]
	if !(end.Op == syntax.OpEndLine || end.Op == syntax.OpEndText) {
		// Regex does not end with $
		return "", false
	}

	if len(re.Sub) == 3 {
		middle := re.Sub[1]
		if middle.Op != syntax.OpLiteral {
			// Regex does not contain a literal op.
			return "", false
		}

		// We can rewrite this regex.
		return string(middle.Rune), true
	}

	// The regex /^$/
	return "", true
}

// ColumnNames will walk all fields and functions and return the appropriate field names for the select statement
// while maintaining order of the field names
func (s *SelectStatement) ColumnNames() []string {
	// First walk each field to determine the number of columns.
	columnFields := Fields{}
	for _, field := range s.Fields {
		columnFields = append(columnFields, field)
	}

	columnNames := make([]string, len(columnFields))
	// Keep track of the encountered column names.
	names := make(map[string]int)

	// Resolve aliases first.
	for i, col := range columnFields {
		if col.Alias != "" {
			columnNames[i] = col.Alias
			names[col.Alias] = 1
		}
	}

	// Resolve any generated names and resolve conflicts.
	for i, col := range columnFields {
		if columnNames[i] != "" {
			continue
		}

		name := col.Name()
		count, conflict := names[name]
		if conflict {
			for {
				resolvedName := fmt.Sprintf("%s_%d", name, count)
				_, conflict = names[resolvedName]
				if !conflict {
					names[name] = count + 1
					name = resolvedName
					break
				}
				count++
			}
		}
		names[name]++
		columnNames[i] = name
	}
	return columnNames
}

// String returns a string representation of the select statement.
func (s *SelectStatement) String() string {
	var buf bytes.Buffer
	_, _ = buf.WriteString("SELECT ")
	_, _ = buf.WriteString(s.Fields.String())

	if len(s.Sources) > 0 {
		_, _ = buf.WriteString(" FROM ")
		_, _ = buf.WriteString(s.Sources.String())
	}
	if s.Condition != nil {
		_, _ = buf.WriteString(" WHERE ")
		_, _ = buf.WriteString(s.Condition.String())
	}
	return buf.String()
}

func (s *SelectStatement) validate() error {
	if err := s.validateFields(); err != nil {
		return err
	}

	if err := s.validateAggregates(); err != nil {
		return err
	}

	return nil
}

func (s *SelectStatement) validateFields() error {
	for _, f := range s.Fields {
		var c validateField
		Walk(&c, f.Expr)
		if c.foundInvalid {
			return fmt.Errorf("invalid operator %s in SELECT field, only support +-*/", c.badToken)
		}
		switch expr := f.Expr.(type) {
		case *BinaryExpr:
			if err := expr.validate(); err != nil {
				return err
			}
		case *ParenExpr:
		case *Call:
		default:
			return fmt.Errorf("invalid field %v in SELECT field, at least one function", expr)
		}
	}
	return nil
}

// validSelectWithAggregate determines if a SELECT statement has the correct
// combination of aggregate functions combined with selected fields and tags
// Currently we don't have support for all aggregates, but aggregates that
// can be combined with fields/tags are:
//  TOP, BOTTOM, MAX, MIN, FIRST, LAST
func (s *SelectStatement) validSelectWithAggregate() error {
	calls := map[string]struct{}{}
	numAggregates := 0
	for _, f := range s.Fields {
		fieldCalls := walkFunctionCalls(f.Expr)
		for _, c := range fieldCalls {
			calls[c.Name] = struct{}{}
		}
		if len(fieldCalls) != 0 {
			numAggregates++
		}
	}
	// For TOP, BOTTOM, MAX, MIN, FIRST, LAST, PERCENTILE (selector functions) it is ok to ask for fields and tags
	// but only if one function is specified.  Combining multiple functions and fields and tags is not currently supported
	onlySelectors := true
	for k := range calls {
		switch k {
		case "top", "bottom", "max", "min", "first", "last", "percentile", "sample":
		default:
			onlySelectors = false
			break
		}
	}
	if onlySelectors {
		// If they only have one selector, they can have as many fields or tags as they want
		if numAggregates == 1 {
			return nil
		}
		// If they have multiple selectors, they are not allowed to have any other fields or tags specified
		if numAggregates > 1 && len(s.Fields) != numAggregates {
			return fmt.Errorf("mixing multiple selector functions with tags or fields is not supported")
		}
	}

	if numAggregates != 0 && numAggregates != len(s.Fields) {
		return fmt.Errorf("mixing aggregate and non-aggregate queries is not supported")
	}
	return nil
}

func (s *SelectStatement) validateAggregates() error {
	for _, f := range s.Fields {
		for _, expr := range walkFunctionCalls(f.Expr) {
			if err := s.validSelectWithAggregate(); err != nil {
				return err
			}
			if len(expr.Args) != 1 {
				return fmt.Errorf("invalid number of arguments for %s, expected 1, got %d", expr.Name, len(expr.Args))
			}
			if expr.Name == "count" {
				if _, ok := expr.Args[0].(*VarRef); !ok {
					return fmt.Errorf("expected only field argument in count()")
				}
			}
			switch fc := expr.Args[0].(type) {
			case *VarRef:
				// do nothing
			case *BinaryExpr:
				if err := fc.validateArgs(); err != nil {
					return err
				}
			default:
				return fmt.Errorf("expected field argument in %s()", expr.Name)
			}
		}
	}
	return nil
}

// NamesInWhere returns the field and tag names (idents) referenced in the where clause
func (s *SelectStatement) NamesInWhere() []string {
	var a []string
	if s.Condition != nil {
		a = walkNames(s.Condition)
	}
	return a
}

// NamesInSelect returns the field and tag names (idents) in the select clause
func (s *SelectStatement) NamesInSelect() []string {
	var a []string

	for _, f := range s.Fields {
		a = append(a, walkNames(f.Expr)...)
	}

	return a
}

// walkNames will walk the Expr and return the database fields
func walkNames(exp Expr) []string {
	switch expr := exp.(type) {
	case *VarRef:
		return []string{expr.Val}
	case *Call:
		var a []string
		for _, expr := range expr.Args {
			if ref, ok := expr.(*VarRef); ok {
				a = append(a, ref.Val)
			}
		}
		return a
	case *BinaryExpr:
		var ret []string
		ret = append(ret, walkNames(expr.LHS)...)
		ret = append(ret, walkNames(expr.RHS)...)
		return ret
	case *ParenExpr:
		return walkNames(expr.Expr)
	}

	return nil
}

// walkRefs will walk the Expr and return the database fields
func walkRefs(exp Expr) []VarRef {
	switch expr := exp.(type) {
	case *VarRef:
		return []VarRef{*expr}
	case *Call:
		a := make([]VarRef, 0, len(expr.Args))
		for _, expr := range expr.Args {
			if ref, ok := expr.(*VarRef); ok {
				a = append(a, *ref)
			}
		}
		return a
	case *BinaryExpr:
		lhs := walkRefs(expr.LHS)
		rhs := walkRefs(expr.RHS)
		ret := make([]VarRef, 0, len(lhs)+len(rhs))
		ret = append(ret, lhs...)
		ret = append(ret, rhs...)
		return ret
	case *ParenExpr:
		return walkRefs(expr.Expr)
	}

	return nil
}

// FunctionCalls returns the Call objects from the query
func (s *SelectStatement) FunctionCalls() []*Call {
	var a []*Call
	for _, f := range s.Fields {
		a = append(a, walkFunctionCalls(f.Expr)...)
	}
	return a
}

// FunctionCalls returns the Call objects from the query
func (s *SelectStatement) EvalFunctionCalls(m map[string]interface{}) {
	for _, f := range s.Fields {
		evalFC(f.Expr, m)
	}
}

func evalFC(expr Expr, m map[string]interface{}) {
	switch expr := expr.(type) {
	case *Call:
		switch expr.Name {
		case "sum", "avg":
			switch res := Eval(expr.Args[0], m).(type) {
			case int64:
				expr.result += float64(res)
			case float64:
				expr.result += res
			}
		}
	case *BinaryExpr:
		evalFC(expr.LHS, m)
		evalFC(expr.RHS, m)
	}
}

type Point struct {
	Metric float64
	TS     int64
}

func (s *SelectStatement) EvalMetric() []Point {
	points := []Point{}
	for _, f := range s.Fields {
		points = append(points, Point{Eval(f.Expr, nil).(float64), time.Now().Unix()})
	}
	return points
}

// FunctionCallsByPosition returns the Call objects from the query in the order they appear in the select statement
func (s *SelectStatement) FunctionCallsByPosition() [][]*Call {
	var a [][]*Call
	for _, f := range s.Fields {
		a = append(a, walkFunctionCalls(f.Expr))
	}
	return a
}

// walkFunctionCalls walks the Field of a query for any function calls made
func walkFunctionCalls(exp Expr) []*Call {
	switch expr := exp.(type) {
	case *VarRef:
		return nil
	case *Call:
		return []*Call{expr}
	case *BinaryExpr:
		var ret []*Call
		ret = append(ret, walkFunctionCalls(expr.LHS)...)
		ret = append(ret, walkFunctionCalls(expr.RHS)...)
		return ret
	case *ParenExpr:
		return walkFunctionCalls(expr.Expr)
	}

	return nil
}

// filters an expression to exclude expressions unrelated to a source.
func filterExprBySource(name string, expr Expr) Expr {
	switch expr := expr.(type) {
	case *VarRef:
		if !strings.HasPrefix(expr.Val, name) {
			return nil
		}

	case *BinaryExpr:
		lhs := filterExprBySource(name, expr.LHS)
		rhs := filterExprBySource(name, expr.RHS)

		// If an expr is logical then return either LHS/RHS or both.
		// If an expr is arithmetic or comparative then require both sides.
		if expr.Op == AND || expr.Op == OR {
			if lhs == nil && rhs == nil {
				return nil
			} else if lhs != nil && rhs == nil {
				return lhs
			} else if lhs == nil && rhs != nil {
				return rhs
			}
		} else {
			if lhs == nil || rhs == nil {
				return nil
			}
		}
		return &BinaryExpr{Op: expr.Op, LHS: lhs, RHS: rhs}

	case *ParenExpr:
		exp := filterExprBySource(name, expr.Expr)
		if exp == nil {
			return nil
		}
		return &ParenExpr{Expr: exp}
	}
	return expr
}

// MatchSource returns the source name that matches a field name.
// Returns a blank string if no sources match.
func MatchSource(sources Sources, name string) string {
	return ""
}

// Fields represents a list of fields.
type Fields []*Field

// AliasNames returns a list of calculated field names in
// order of alias, function name, then field.
func (a Fields) AliasNames() []string {
	names := []string{}
	for _, f := range a {
		names = append(names, f.Name())
	}
	return names
}

// Names returns a list of field names.
func (a Fields) Names() []string {
	names := []string{}
	for _, f := range a {
		switch expr := f.Expr.(type) {
		case *Call:
			names = append(names, expr.Name)
		case *VarRef:
			names = append(names, expr.Val)
		case *BinaryExpr:
			names = append(names, walkNames(expr)...)
		case *ParenExpr:
			names = append(names, walkNames(expr)...)
		}
	}
	return names
}

// String returns a string representation of the fields.
func (a Fields) String() string {
	var str []string
	for _, f := range a {
		str = append(str, f.String())
	}
	return strings.Join(str, ", ")
}

// Field represents an expression retrieved from a select statement.
type Field struct {
	Expr  Expr
	Alias string
}

// Name returns the name of the field. Returns alias, if set.
// Otherwise uses the function name or variable name.
func (f *Field) Name() string {
	// Return alias, if set.
	if f.Alias != "" {
		return f.Alias
	}

	// Return the function name or variable name, if available.
	switch expr := f.Expr.(type) {
	case *Call:
		return expr.Name
	case *BinaryExpr:
		return BinaryExprName(expr)
	case *ParenExpr:
		f := Field{Expr: expr.Expr}
		return f.Name()
	case *VarRef:
		return expr.Val
	}

	// Otherwise return a blank name.
	return ""
}

// String returns a string representation of the field.
func (f *Field) String() string {
	str := f.Expr.String()

	if f.Alias == "" {
		return str
	}
	return fmt.Sprintf("%s AS %s", str, QuoteIdent(f.Alias))
}

// Sort Interface for Fields
func (a Fields) Len() int           { return len(a) }
func (a Fields) Less(i, j int) bool { return a[i].Name() < a[j].Name() }
func (a Fields) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

// VarRef represents a reference to a variable.
type VarRef struct {
	Val      string
	Segments []string
}

// String returns a string representation of the variable reference.
func (r *VarRef) String() string {
	buf := bytes.NewBufferString(QuoteIdent(r.Val))
	return buf.String()
}

// VarRefs represents a slice of VarRef types.
type VarRefs []VarRef

func (a VarRefs) Len() int { return len(a) }

func (a VarRefs) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

// Strings returns a slice of the variable names.
func (a VarRefs) Strings() []string {
	s := make([]string, len(a))
	for i, ref := range a {
		s[i] = ref.Val
	}
	return s
}

// Call represents a function call.
type Call struct {
	Name   string
	Args   []Expr  // must hava not funcCall expr
	result float64 // must be float64
}

// String returns a string representation of the call.
func (c *Call) String() string {
	// Join arguments.
	var str []string
	for _, arg := range c.Args {
		str = append(str, arg.String())
	}

	// Write function name and args.
	return fmt.Sprintf("%s(%s)", c.Name, strings.Join(str, ", "))
}

// Fields will extract any field names from the call.  Only specific calls support this.
func (c *Call) Fields() []string {
	switch c.Name {
	case "top", "bottom":
		// maintain the order the user specified in the query
		keyMap := make(map[string]struct{})
		keys := []string{}
		for i, a := range c.Args {
			if i == 0 {
				// special case, first argument is always the name of the function regardless of the field name
				keys = append(keys, c.Name)
				continue
			}
			switch v := a.(type) {
			case *VarRef:
				if _, ok := keyMap[v.Val]; !ok {
					keyMap[v.Val] = struct{}{}
					keys = append(keys, v.Val)
				}
			}
		}
		return keys
	case "min", "max", "first", "last", "sum", "mean", "mode":
		// maintain the order the user specified in the query
		keyMap := make(map[string]struct{})
		keys := []string{}
		for _, a := range c.Args {
			switch v := a.(type) {
			case *VarRef:
				if _, ok := keyMap[v.Val]; !ok {
					keyMap[v.Val] = struct{}{}
					keys = append(keys, v.Val)
				}
			}
		}
		return keys
	default:
		panic(fmt.Sprintf("*call.Fields is unable to provide information on %s", c.Name))
	}
}

// NumberLiteral represents a numeric literal.
type NumberLiteral struct {
	Val float64
}

// String returns a string representation of the literal.
func (l *NumberLiteral) String() string { return strconv.FormatFloat(l.Val, 'f', 3, 64) }

// IntegerLiteral represents an integer literal.
type IntegerLiteral struct {
	Val int64
}

// String returns a string representation of the literal.
func (l *IntegerLiteral) String() string { return fmt.Sprintf("%d", l.Val) }

// BooleanLiteral represents a boolean literal.
type BooleanLiteral struct {
	Val bool
}

// String returns a string representation of the literal.
func (l *BooleanLiteral) String() string {
	if l.Val {
		return "true"
	}
	return "false"
}

// isTrueLiteral returns true if the expression is a literal "true" value.
func isTrueLiteral(expr Expr) bool {
	if expr, ok := expr.(*BooleanLiteral); ok {
		return expr.Val == true
	}
	return false
}

// isFalseLiteral returns true if the expression is a literal "false" value.
func isFalseLiteral(expr Expr) bool {
	if expr, ok := expr.(*BooleanLiteral); ok {
		return expr.Val == false
	}
	return false
}

// ListLiteral represents a list of strings literal.
type ListLiteral struct {
	Vals []interface{}
}

// String returns a string representation of the literal.
func (s *ListLiteral) String() string {
	var buf bytes.Buffer
	_, _ = buf.WriteString("[")
	for idx, tagKey := range s.Vals {
		if idx != 0 {
			_, _ = buf.WriteString(", ")
		}
		switch v := tagKey.(type) {
		case string:
			_, _ = buf.WriteString(QuoteIdent(v))
		case float64:
			_, _ = buf.WriteString((fmt.Sprintf("%f", v)))
		case int64:
			_, _ = buf.WriteString((fmt.Sprintf("%d", v)))
		}
	}
	_, _ = buf.WriteString("]")
	return buf.String()
}

// StringLiteral represents a string literal.
type StringLiteral struct {
	Val string
}

// String returns a string representation of the literal.
func (l *StringLiteral) String() string { return QuoteString(l.Val) }

// nilLiteral represents a nil literal.
// This is not available to the query language itself. It's only used internally.
type nilLiteral struct{}

// String returns a string representation of the literal.
func (l *nilLiteral) String() string { return `nil` }

// BinaryExpr represents an operation between two expressions.
type BinaryExpr struct {
	Op  Token
	LHS Expr
	RHS Expr
}

// String returns a string representation of the binary expression.
func (e *BinaryExpr) String() string {
	return fmt.Sprintf("%s %s %s", e.LHS.String(), e.Op.String(), e.RHS.String())
}

func (e *BinaryExpr) validate() error {
	v := binaryExprValidator{}
	Walk(&v, e)
	if v.err != nil {
		return v.err
	} else if v.calls && v.refs {
		return errors.New("binary expressions cannot mix aggregates and raw fields")
	}
	return nil
}

func (e *BinaryExpr) validateArgs() error {
	v := binaryExprValidator{}
	Walk(&v, e)
	if v.err != nil {
		return v.err
	} else if v.calls {
		return errors.New("argument binary expressions cannot mix function")
	} else if !v.refs {
		return errors.New("argument binary expressions at least one key")
	}
	return nil
}

type binaryExprValidator struct {
	calls bool
	refs  bool
	err   error
}

func (v *binaryExprValidator) Visit(n Node) Visitor {
	if v.err != nil {
		return nil
	}

	switch n := n.(type) {
	case *Call:
		v.calls = true
		for _, expr := range n.Args {
			switch e := expr.(type) {
			case *BinaryExpr:
				v.err = e.validate()
				return nil
			}
		}
		return nil
	case *VarRef:
		v.refs = true
		return nil
	}
	return v
}

// BinaryExprName returns the name of a binary expression by concatenating
// the variables in the binary expression with underscores.
func BinaryExprName(expr *BinaryExpr) string {
	v := binaryExprNameVisitor{}
	Walk(&v, expr)
	return strings.Join(v.names, "_")
}

type binaryExprNameVisitor struct {
	names []string
}

func (v *binaryExprNameVisitor) Visit(n Node) Visitor {
	switch n := n.(type) {
	case *VarRef:
		v.names = append(v.names, n.Val)
	case *Call:
		v.names = append(v.names, n.Name)
		return nil
	}
	return v
}

// ParenExpr represents a parenthesized expression.
type ParenExpr struct {
	Expr Expr
}

// String returns a string representation of the parenthesized expression.
func (e *ParenExpr) String() string { return fmt.Sprintf("(%s)", e.Expr.String()) }

// RegexLiteral represents a regular expression.
type RegexLiteral struct {
	Val *regexp.Regexp
}

// String returns a string representation of the literal.
func (r *RegexLiteral) String() string {
	if r.Val != nil {
		return fmt.Sprintf("/%s/", strings.Replace(r.Val.String(), `/`, `\/`, -1))
	}
	return ""
}

// CloneRegexLiteral returns a clone of the RegexLiteral.
func CloneRegexLiteral(r *RegexLiteral) *RegexLiteral {
	if r == nil {
		return nil
	}

	clone := &RegexLiteral{}
	if r.Val != nil {
		clone.Val = regexp.MustCompile(r.Val.String())
	}

	return clone
}

// Wildcard represents a wild card expression.
type Wildcard struct {
	Type Token
}

// HasTimeExpr returns true if the expression has a time term.
func HasTimeExpr(expr Expr) bool {
	switch n := expr.(type) {
	case *BinaryExpr:
		if n.Op == AND || n.Op == OR {
			return HasTimeExpr(n.LHS) || HasTimeExpr(n.RHS)
		}
		if ref, ok := n.LHS.(*VarRef); ok && strings.ToLower(ref.Val) == "time" {
			return true
		}
		return false
	case *ParenExpr:
		// walk down the tree
		return HasTimeExpr(n.Expr)
	default:
		return false
	}
}

// OnlyTimeExpr returns true if the expression only has time constraints.
func OnlyTimeExpr(expr Expr) bool {
	if expr == nil {
		return false
	}
	switch n := expr.(type) {
	case *BinaryExpr:
		if n.Op == AND || n.Op == OR {
			return OnlyTimeExpr(n.LHS) && OnlyTimeExpr(n.RHS)
		}
		if ref, ok := n.LHS.(*VarRef); ok && strings.ToLower(ref.Val) == "time" {
			return true
		}
		return false
	case *ParenExpr:
		// walk down the tree
		return OnlyTimeExpr(n.Expr)
	default:
		return false
	}
}

// Visitor can be called by Walk to traverse an AST hierarchy.
// The Visit() function is called once per node.
type Visitor interface {
	Visit(Node) Visitor
}

// Walk traverses a node hierarchy in depth-first order.
func Walk(v Visitor, node Node) {
	if node == nil {
		return
	}

	if v = v.Visit(node); v == nil {
		return
	}

	switch n := node.(type) {
	case *BinaryExpr:
		Walk(v, n.LHS)
		Walk(v, n.RHS)

	case *Call:
		for _, expr := range n.Args {
			Walk(v, expr)
		}

	case *Field:
		Walk(v, n.Expr)

	case Fields:
		for _, c := range n {
			Walk(v, c)
		}

	case *ParenExpr:
		Walk(v, n.Expr)

	case *SelectStatement:
		Walk(v, n.Fields)
		Walk(v, n.Sources)
		Walk(v, n.Condition)

	case Sources:
		for _, s := range n {
			Walk(v, s)
		}

	case Statements:
		for _, s := range n {
			Walk(v, s)
		}

	}
}

// WalkFunc traverses a node hierarchy in depth-first order.
func WalkFunc(node Node, fn func(Node)) {
	Walk(walkFuncVisitor(fn), node)
}

type walkFuncVisitor func(Node)

func (fn walkFuncVisitor) Visit(n Node) Visitor { fn(n); return fn }

// Rewriter can be called by Rewrite to replace nodes in the AST hierarchy.
// The Rewrite() function is called once per node.
type Rewriter interface {
	Rewrite(Node) Node
}

// Eval evaluates expr against a map.
func Eval(expr Expr, m map[string]interface{}) interface{} {
	if expr == nil {
		return nil
	}

	switch expr := expr.(type) {
	case *Call:
		res := expr.result
		expr.result = 0
		return res
	case *BinaryExpr:
		return evalBinaryExpr(expr, m)
	case *BooleanLiteral:
		return expr.Val
	case *ListLiteral:
		return expr.Vals
	case *IntegerLiteral:
		return expr.Val
	case *NumberLiteral:
		return expr.Val
	case *ParenExpr:
		return Eval(expr.Expr, m)
	case *RegexLiteral:
		return expr.Val
	case *StringLiteral:
		return expr.Val
	case *VarRef:
		ms, _ := json.Marshal(m)
		js, _ := simplejson.NewJson(ms)
		switch v := js.GetPath(expr.Segments...).Interface().(type) {
		case json.Number:
			if n, err := v.Int64(); err != nil {
				if f, err := v.Float64(); err != nil {
					fmt.Println("json Number eval Error")
				} else {
					return f
				}
			} else {
				return n
			}
		default:
			return v
		}
	default:
		return nil
	}
	return nil
}

func evalBinaryExpr(expr *BinaryExpr, m map[string]interface{}) interface{} {
	lhs := Eval(expr.LHS, m)
	rhs := Eval(expr.RHS, m)

	// Evaluate if both sides are simple types.
	switch lhs := lhs.(type) {
	case bool:
		rhs, ok := rhs.(bool)
		switch expr.Op {
		case AND:
			return ok && (lhs && rhs)
		case OR:
			return ok && (lhs || rhs)
		case EQ:
			return ok && (lhs == rhs)
		case NEQ:
			return ok && (lhs != rhs)
		}
	case float64:
		// Try the rhs as a float64 or int64
		rhsf, ok := rhs.(float64)
		if !ok {
			var rhsi int64
			if rhsi, ok = rhs.(int64); ok {
				rhsf = float64(rhsi)
			}
		}

		switch expr.Op {
		case IN:
			return in_array(lhs, rhs)
		case NI:
			return !in_array(lhs, rhs)
		case EQ:
			return ok && (lhs == rhsf)
		case NEQ:
			return ok && (lhs != rhsf)
		case LT:
			return ok && (lhs < rhsf)
		case LTE:
			return ok && (lhs <= rhsf)
		case GT:
			return ok && (lhs > rhsf)
		case GTE:
			return ok && (lhs >= rhsf)
		case ADD:
			if !ok {
				return nil
			}
			return lhs + rhsf
		case SUB:
			if !ok {
				return nil
			}
			return lhs - rhsf
		case MUL:
			if !ok {
				return nil
			}
			return lhs * rhsf
		case DIV:
			if !ok {
				return nil
			} else if rhs == 0 {
				return float64(0)
			}
			return lhs / rhsf
		}
	case int64:
		// Try as a float64 to see if a float cast is required.
		rhsf, ok := rhs.(float64)
		if ok {
			lhs := float64(lhs)
			rhs := rhsf
			switch expr.Op {
			case EQ:
				return lhs == rhs
			case NEQ:
				return lhs != rhs
			case LT:
				return lhs < rhs
			case LTE:
				return lhs <= rhs
			case GT:
				return lhs > rhs
			case GTE:
				return lhs >= rhs
			case ADD:
				return lhs + rhs
			case SUB:
				return lhs - rhs
			case MUL:
				return lhs * rhs
			case DIV:
				if rhs == 0 {
					return float64(0)
				}
				return lhs / rhs
			}
		} else {
			rhsi, ok := rhs.(int64)
			switch expr.Op {
			case IN:
				return in_array(lhs, rhs)
			case NI:
				return !in_array(lhs, rhs)
			case EQ:
				return ok && (lhs == rhsi)
			case NEQ:
				return ok && (lhs != rhsi)
			case LT:
				return ok && (lhs < rhsi)
			case LTE:
				return ok && (lhs <= rhsi)
			case GT:
				return ok && (lhs > rhsi)
			case GTE:
				return ok && (lhs >= rhsi)
			case ADD:
				if !ok {
					return nil
				}
				return lhs + rhsi
			case SUB:
				if !ok {
					return nil
				}
				return lhs - rhsi
			case MUL:
				if !ok {
					return nil
				}
				return lhs * rhsi
			case DIV:
				if !ok {
					return nil
				} else if rhs == 0 {
					return float64(0)
				}
				return lhs / rhsi
			}
		}
	case string:
		switch expr.Op {
		case IN:
			return in_array(lhs, rhs)
		case NI:
			return !in_array(lhs, rhs)
		case EQ:
			rhs, ok := rhs.(string)
			return ok && lhs == rhs
		case NEQ:
			rhs, ok := rhs.(string)
			return ok && lhs != rhs
		case EQREGEX:
			rhs, ok := rhs.(*regexp.Regexp)
			return ok && rhs.MatchString(lhs)
		case NEQREGEX:
			rhs, ok := rhs.(*regexp.Regexp)
			return ok && !rhs.MatchString(lhs)
		}
	}
	return nil
}

func in_array(val interface{}, array interface{}) (exists bool) {
	exists = false

	switch reflect.TypeOf(array).Kind() {
	case reflect.Slice:
		s := reflect.ValueOf(array)

		for i := 0; i < s.Len(); i++ {
			if reflect.DeepEqual(val, s.Index(i).Interface()) == true {
				exists = true
				return
			}
		}
	}
	return
}

// EvalBool evaluates expr and returns true if result is a boolean true.
// Otherwise returns false.
func EvalBool(expr Expr, m map[string]interface{}) bool {
	v, _ := Eval(expr, m).(bool)
	return v
}

// Valuer is the interface that wraps the Value() method.
//
// Value returns the value and existence flag for a given key.
type Valuer interface {
	Value(key string) (interface{}, bool)
}

// NowValuer returns only the value for "now()".
type NowValuer struct {
	Now time.Time
}

// Value is a method that returns the value and existence flag for a given key.
func (v *NowValuer) Value(key string) (interface{}, bool) {
	if key == "now()" {
		return v.Now, true
	}
	return nil, false
}

// ContainsVarRef returns true if expr is a VarRef or contains one.
func ContainsVarRef(expr Expr) bool {
	var v containsVarRefVisitor
	Walk(&v, expr)
	return v.contains
}

type containsVarRefVisitor struct {
	contains bool
}

func (v *containsVarRefVisitor) Visit(n Node) Visitor {
	switch n.(type) {
	case *Call:
		return nil
	case *VarRef:
		v.contains = true
	}
	return v
}

// Measurements represents a list of measurements.
type Measurements []*Measurement

// String returns a string representation of the measurements.
func (a Measurements) String() string {
	var str []string
	for _, m := range a {
		str = append(str, m.String())
	}
	return strings.Join(str, ", ")
}

// Measurement represents a single measurement used as a datasource.
type Measurement struct {
	Database string
}

// String returns a string representation of the measurement.
func (m *Measurement) String() string {
	return m.Database
}
