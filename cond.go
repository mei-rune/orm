package orm

import (
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/go-xorm/builder"
)

// Cond is a map that defines conditions for a query and satisfies the
// Constraints and Compound interfaces.
//
// Each entry of the map represents a condition (a column-value relation bound
// by a comparison operator). The comparison operator is optional and can be
// specified after the column name, if no comparison operator is provided the
// equality is used.
//
// Examples:
//
//  // Where age equals 18.
//  db.Cond{"age": 18}
//  //	// Where age is greater than or equal to 18.
//  db.Cond{"age >=": 18}
//
//  // Where id is in a list of ids.
//  db.Cond{"id IN": []{1, 2, 3}}
//
//  // Where age > 32 and age < 35
//  db.Cond{"age >": 32, "age <": 35}
type Cond map[string]interface{}

func toConds(constraints Cond, concat func(conds ...builder.Cond) builder.Cond) builder.Cond {
	if len(constraints) == 0 {
		panic(errors.New("constraints is empty"))
	}
	if len(constraints) == 1 {
		for k, v := range constraints {
			if v == nil {
				return builder.Expr(k)
			}
			ss := strings.Fields(k)
			switch len(ss) {
			case 1:
				lower := strings.ToLower(k)
				if lower == "not" {
					if cond, ok := v.(Cond); ok {
						return builder.Not{toConds(cond, concat)}
					}
					panic(fmt.Errorf("unknow cond expression - \"%s %#v\"", k, v))
				}
				if lower == "or" {
					if cond, ok := v.(Cond); ok {
						return toConds(cond, builder.Or)
					}
					panic(fmt.Errorf("unknow cond expression - \"%s %#v\"", k, v))
				}

				return builder.Eq(constraints)
			case 2:
				return toCond(ss[0], ss[1], v)
			case 3:
				if strings.ToLower(ss[1]) == "not" {
					cond := toNotCond(ss[0], ss[2], v)
					if cond != nil {
						return cond
					}
				}
				panic(fmt.Errorf("unknow cond expression - \"%s %#v\"", k, v))
			default:
				if strings.ToLower(ss[0]) == "exists" {
					return toExists(k, v)
				}
				panic(fmt.Errorf("unknow cond expression - \"%s %#v\"", k, v))
			}
		}
	}

	var deleted []string
	var conds = make([]builder.Cond, 0, len(constraints))
	for k, v := range constraints {
		if v == nil {
			deleted = append(deleted, k)
			conds = append(conds, builder.Expr(k))
			continue
		}

		ss := strings.Fields(k)
		switch len(ss) {
		case 1:
			if strings.ToLower(k) == "not" {
				if cond, ok := v.(Cond); ok {
					deleted = append(deleted, k)
					conds = append(conds, builder.Not{toConds(cond, concat)})
				} else {
					panic(fmt.Errorf("unknow cond expression - \"%s %#v\"", k, v))
				}
			}

			if strings.ToLower(k) == "or" {
				if cond, ok := v.(Cond); ok {
					deleted = append(deleted, k)
					conds = append(conds, toConds(cond, builder.Or))
				} else {
					panic(fmt.Errorf("unknow cond expression - \"%s %#v\"", k, v))
				}
			}
		case 2:
			deleted = append(deleted, k)
			conds = append(conds, toCond(ss[0], ss[1], v))
		case 3:
			deleted = append(deleted, k)
			if strings.ToLower(ss[1]) == "not" {
				cond := toNotCond(ss[0], ss[2], v)
				if cond != nil {
					conds = append(conds, cond)
					break
				}
			}
			panic(fmt.Errorf("unknow cond expression - \"%s %#v\"", k, v))
		default:
			if lower := strings.ToLower(ss[0]); lower == "exists" || lower == "exists(" {
				conds = append(conds, toExists(k, v))
				break
			}
			panic(fmt.Errorf("unknow cond expression - \"%s %#v\"", k, v))
		}
	}
	deletedSize := len(deleted)
	if deletedSize == 0 {
		if concat != nil {
			conds = make([]builder.Cond, 0, len(constraints))
			for k, v := range constraints {
				conds = append(conds, builder.Eq{k: v})
			}
			return concat(conds...)
		}
		return builder.Eq(constraints)
	}

	if len(constraints) != len(conds) {
		newCopy := map[string]interface{}{}
		for k, v := range constraints {
			found := false
			for _, s := range deleted {
				if k == s {
					found = true
					break
				}
			}
			if !found {
				newCopy[k] = v
			}
		}
		conds = append(conds, builder.Eq(newCopy))
	}

	if concat != nil {
		return concat(conds...)
	}
	return builder.And(conds...)
}

func toNotCond(name, op string, value interface{}) builder.Cond {
	switch strings.ToLower(op) {
	case "in":
		return builder.NotIn(name, value)
	case "like":
		if s, ok := value.(string); ok {
			return builder.Not{builder.Like{name, s}}
		}
	}
	return nil
}

func toExists(expr string, value interface{}) builder.Cond {
	return builder.Expr(expr, toArray(value)...)
}

func toArray(value interface{}) []interface{} {
	if values, ok := value.([]interface{}); ok {
		return values
	}

	v := reflect.ValueOf(value)
	if v.Kind() != reflect.Slice {
		return []interface{}{value}
	}
	l := v.Len()
	if l == 0 {
		return []interface{}{}
	}

	values := make([]interface{}, 0, l)
	for i := 0; i < l; i++ {
		values = append(values, v.Index(i).Interface())
	}
	return values
}

func toCond(name, op string, value interface{}) builder.Cond {
	switch op {
	case "=":
		return builder.Eq{name: value}
	case "<>":
		return builder.Neq{name: value}
	case "<":
		return builder.Lt{name: value}
	case "<=":
		return builder.Lte{name: value}
	case ">":
		return builder.Gt{name: value}
	case ">=":
		return builder.Gte{name: value}
	case "is", "IS", "Is":
		if s, ok := value.(string); ok {
			ss := strings.Fields(s)
			switch len(ss) {
			case 1:
				if strings.ToLower(ss[0]) == "null" {
					return builder.IsNull{name}
				}
			case 2:
				if strings.ToLower(ss[0]) == "not" && strings.ToLower(ss[1]) == "null" {
					return builder.NotNull{name}
				}
			}
		}
	case "in", "In", "IN":
		return builder.In(name, value)
	case "like", "Like", "LIKE":
		if s, ok := value.(string); ok {
			return builder.Like{name, s}
		}
	case "between", "Between", "BETWEEN":
		v := reflect.ValueOf(value)
		if v.Kind() == reflect.Slice {
			l := v.Len()
			if l == 2 {
				return builder.Between{Col: name,
					LessVal: v.Index(0).Interface(),
					MoreVal: v.Index(1).Interface()}
			}
		}
	}
	panic(fmt.Errorf("unknow cond expression - \"%s %s %#v\"", name, op, value))
}
