package jepl

import (
	"fmt"
	"github.com/buger/jsonparser"
	"reflect"
	"regexp"
	"time"
)

// Points is a slice timeseries metric valus
type Points []point

type point struct {
	Metric float64
	TS     int64
}

func (s *SelectStatement) evalMetric() Points {
	ps := []point{}
	for _, f := range s.Fields {
		ps = append(ps, point{Eval(f.Expr, nil).(float64), time.Now().Unix()})
	}
	return ps
}

//EvalSQL return metric points map[filter]metric
func EvalSQL(sql string, docs []string) map[string]Points {
	stmt, err := ParseStatement(sql)
	if err != nil {
		panic(err)
	}
	selectStmt, ok := stmt.(*SelectStatement)
	if !ok {
		panic("Not support stmt")
	}

	pm := make(map[string]Points)

	selectStmts := make(map[string]*SelectStatement)
	selectStmts[selectStmt.Condition.String()] = selectStmt

	if len(selectStmt.Dimensions) > 0 {
		selectStmts = selectStmt.FlatStatByGroup(docs)
	}

	for k, st := range selectStmts {
		for _, doc := range docs {
			switch res := Eval(st.Condition, &doc).(type) {
			case bool:
				if res == true {
					st.evalFunctionCalls(&doc)
				}
			default:
				fmt.Println("Select Where Condition parse error")
			}
		}
		ms := st.evalMetric()
		pm[k] = ms
	}
	return pm
}

// Eval evaluates expr against a map.
func Eval(expr Expr, js *string) interface{} {
	if expr == nil {
		return nil
	}

	switch expr := expr.(type) {
	case *Call:
		var ret interface{}

		if expr.Name == "count" {
			ret = float64(expr.Count)
		} else {
			ret = expr.result
			if expr.Name == "avg" {
				if expr.Count > 0 {
					ret = expr.result / float64(expr.Count)
				}
			}
		}

		expr.result = 0.0
		expr.First = true
		expr.Count = 0

		return ret
	case *BinaryExpr:
		return evalBinaryExpr(expr, js)
	case *BooleanLiteral:
		return expr.Val
	case *ListLiteral:
		return expr.Vals
	case *IntegerLiteral:
		return expr.Val
	case *NumberLiteral:
		return expr.Val
	case *ParenExpr:
		return Eval(expr.Expr, js)
	case *RegexLiteral:
		return expr.Val
	case *StringLiteral:
		return expr.Val
	case *VarRef:
		if val, dt, _, err := jsonparser.Get([]byte(*js), expr.Segments...); err == nil {
			switch dt {
			case jsonparser.Number:
				v, _ := jsonparser.ParseFloat(val)
				return v

			case jsonparser.String:
				v, _ := jsonparser.ParseString(val)
				return v

			case jsonparser.Boolean:
				v, _ := jsonparser.ParseBoolean(val)
				return v

			default:
				return nil
			}
		} else {
			fmt.Println(err, expr.Segments)
			return nil
		}
	default:
		return nil
	}

}

func evalBinaryExpr(expr *BinaryExpr, js *string) interface{} {
	lhs := Eval(expr.LHS, js)
	rhs := Eval(expr.RHS, js)

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
			return inList(lhs, rhs)
		case NI:
			return !inList(lhs, rhs)
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
				return inList(lhs, rhs)
			case NI:
				return !inList(lhs, rhs)
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
			return inList(lhs, rhs)
		case NI:
			return !inList(lhs, rhs)
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

// EvalBool evaluates expr and returns true if result is a boolean true.
// Otherwise returns false.
func EvalBool(expr Expr, js *string) bool {
	v, _ := Eval(expr, js).(bool)
	return v
}

// FunctionCalls returns the Call objects from the query
func (s *SelectStatement) evalFunctionCalls(js *string) {
	for _, f := range s.Fields {
		evalFC(f.Expr, js)
	}
}

func evalFC(expr Expr, js *string) {
	switch expr := expr.(type) {
	case *Call:
		expr.Count++

		switch expr.Name {
		case "sum", "avg":
			switch res := Eval(expr.Args[0], js).(type) {
			case int64:
				expr.result += float64(res)
			case float64:
				expr.result += res
			}
		case "max":
			var thisret float64
			switch res := Eval(expr.Args[0], js).(type) {
			case int64:
				thisret = float64(res)
			case float64:
				thisret = res
			}
			if expr.First {
				expr.result = thisret
				expr.First = false
			} else {
				if thisret > expr.result {
					expr.result = thisret
				}
			}

		case "min":
			var thisret float64
			switch res := Eval(expr.Args[0], js).(type) {
			case int64:
				thisret = float64(res)
			case float64:
				thisret = res
			}
			if expr.First {
				expr.result = thisret
				expr.First = false
			} else {
				if thisret < expr.result {
					expr.result = thisret
				}
			}

		}
	case *BinaryExpr:
		evalFC(expr.LHS, js)
		evalFC(expr.RHS, js)
	}
}

func inList(val interface{}, array interface{}) (exists bool) {
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
