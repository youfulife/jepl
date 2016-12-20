package jepl

import (
	"regexp"
)

//FlatStatByGroup divergent multi SelectStatement based on group by clause
func (s *SelectStatement) FlatStatByGroup(docs []string) map[string]*SelectStatement {
	var groups = make(map[string]*BinaryExpr)
	m := make(map[string]*SelectStatement)
	for _, doc := range docs {
		// Dummy root node.
		root := &BinaryExpr{}

		for _, dimension := range s.Dimensions {

			res := Eval(dimension.Expr, &doc)
			var lhs Expr
			switch v := res.(type) {
			case string:
				lhs = &StringLiteral{Val: v}
			case float64:
				lhs = &NumberLiteral{Val: v}
			case bool:
				lhs = &BooleanLiteral{Val: v}
			default:
			}
			rhs := &BinaryExpr{LHS: lhs, Op: EQ, RHS: dimension.Expr}

			if root.LHS == nil {
				root = &BinaryExpr{LHS: &BooleanLiteral{Val: true}, Op: AND, RHS: rhs}
			} else {
				root = &BinaryExpr{LHS: root, Op: AND, RHS: rhs}
			}
		}
		root = &BinaryExpr{LHS: root, Op: AND, RHS: s.Condition}
		groups[root.String()] = root
	}

	for k, v := range groups {
		m[k] = s.Clone()
		m[k].Condition = v
	}

	return m
}

// Clone returns a deep copy of the statement.
func (s *SelectStatement) Clone() *SelectStatement {
	clone := *s
	clone.Fields = make(Fields, 0, len(s.Fields))
	clone.Dimensions = make(Dimensions, 0, len(s.Dimensions))
	clone.Sources = cloneSources(s.Sources)
	clone.Condition = CloneExpr(s.Condition)

	for _, f := range s.Fields {
		clone.Fields = append(clone.Fields, &Field{Expr: CloneExpr(f.Expr), Alias: f.Alias})
	}
	for _, d := range s.Dimensions {
		clone.Dimensions = append(clone.Dimensions, &Dimension{Expr: CloneExpr(d.Expr)})
	}

	return &clone
}

func cloneSources(sources Sources) Sources {
	clone := make(Sources, 0, len(sources))
	for _, s := range sources {
		clone = append(clone, cloneSource(s))
	}
	return clone
}

func cloneSource(s Source) Source {
	if s == nil {
		return nil
	}

	switch s := s.(type) {
	case *Measurement:
		m := &Measurement{Database: s.Database}
		return m
	default:
		panic("unreachable")
	}
}

// CloneExpr returns a deep copy of the expression.
func CloneExpr(expr Expr) Expr {
	if expr == nil {
		return nil
	}
	switch expr := expr.(type) {
	case *BinaryExpr:
		return &BinaryExpr{Op: expr.Op, LHS: CloneExpr(expr.LHS), RHS: CloneExpr(expr.RHS)}
	case *BooleanLiteral:
		return &BooleanLiteral{Val: expr.Val}
	case *Call:
		args := make([]Expr, len(expr.Args))
		for i, arg := range expr.Args {
			args[i] = CloneExpr(arg)
		}
		return &Call{Name: expr.Name, Args: args}
	case *IntegerLiteral:
		return &IntegerLiteral{Val: expr.Val}
	case *NumberLiteral:
		return &NumberLiteral{Val: expr.Val}
	case *ParenExpr:
		return &ParenExpr{Expr: CloneExpr(expr.Expr)}
	case *RegexLiteral:
		return &RegexLiteral{Val: expr.Val}
	case *StringLiteral:
		return &StringLiteral{Val: expr.Val}
	case *VarRef:
		return &VarRef{Val: expr.Val, Segments: expr.Segments[:]}
	}
	panic("unreachable")
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
