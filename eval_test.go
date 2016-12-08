package jepl_test

import (
	"encoding/json"
	"fmt"
	"github.com/bitly/go-simplejson"
	"github.com/chenyoufu/jepl"
	"reflect"
	"testing"
)

func TestTypeValid(t *testing.T) {
	var tests = []struct {
		s    string
		stmt jepl.Statement
		err  string
	}{
		{s: `select max(tcp.in_pkts) from packetbeat where uid > 5 * xxx`, err: ``},
		{s: `select max(tcp.in_pkts) from packetbeat where uid = 'xxx'`, err: ``},
		{s: `select max(tcp.in_pkts) from packetbeat where uid != 'xxx'`, err: ``},
		{s: `select max(tcp.in_pkts) from packetbeat where uid != "xxx"`, err: ``},
		{s: `select max(tcp.in_pkts) from packetbeat where uid = "xxx"`, err: ``},
	}
	for i, test := range tests {
		stmt, err := jepl.ParseStatement(test.s)
		if !reflect.DeepEqual(test.err, errstring(err)) {
			t.Errorf("%d. %q: error mismatch:\n  exp=%s\n  got=%s\n\n", i, test.s, test.err, err)
		}
		json.MarshalIndent(stmt, "", "  ")
		// fmt.Println(string(js))
	}
}

func TestTypeInvalid(t *testing.T) {
	var tests = []struct {
		s    string
		stmt jepl.Statement
		err  string
	}{
		{s: `select max(tcp.in_pkts) from packetbeat where uid > 'xxx'`, err: `invalid filter, unsupport op > for string`},
		{s: `select max(tcp.in_pkts) from packetbeat where uid < "xxx"`, err: `invalid filter, unsupport op < for string`},
		{s: `select max(tcp.in_pkts) from packetbeat where uid >= 'xxx'`, err: `invalid filter, unsupport op >= for string`},
		{s: `select max(tcp.in_pkts) from packetbeat where uid <= 'xxx'`, err: `invalid filter, unsupport op <= for string`},
		{s: `select max(tcp.in_pkts) from packetbeat where uid <= "xxx"`, err: `invalid filter, unsupport op <= for string`},
		{s: `select max(tcp.in_pkts) from packetbeat where uid = "xxx" AND xx > "yyy"`, err: `invalid filter, unsupport op > for string`},
		{s: `select max(tcp.in_pkts) from packetbeat where uid = 5 * "xxx" + "xxx"`, err: `invalid filter, unsupport op * for string`},
	}
	for i, test := range tests {
		_, err := jepl.ParseStatement(test.s)
		if !reflect.DeepEqual(test.err, errstring(err)) {
			t.Errorf("%d. %q: error mismatch:\n  exp=%s\n  got=%s\n\n", i, test.s, test.err, err)
		}
	}
}

func TestSyntax(t *testing.T) {
	var tests = []struct {
		s    string
		stmt jepl.Statement
		err  string
	}{
		{s: `select max(tcp.in_pkts) from packetbeat where uid = 1`, err: ``},
		{s: `select avg(tcp.in_pkts) from packetbeat  `, err: ``},
		{s: `select sum(tcp.in_pkts) from packetbeat  uid = 1`, err: `found uid, expected EOF at line 1, char 42`},
	}
	for i, test := range tests {
		_, err := jepl.ParseStatement(test.s)
		if !reflect.DeepEqual(test.err, errstring(err)) {
			t.Errorf("%d. %q: error mismatch:\n  exp=%s\n  got=%s\n\n", i, test.s, test.err, err)
		}
	}
}

func TestEvalQuery(t *testing.T) {
	s := "select max(tcp.in_pkts), min(tcp.in_pkts), count(tcp.in_pkts), sum(tcp.in_pkts), avg(tcp.in_pkts) from packetbeat where uid = 1"

	stmt, err := jepl.ParseStatement(s)
	if err != nil {
		panic(err)
	}
	cond := stmt.(*jepl.SelectStatement).Condition
	// fields := stmt.(*jepl.SelectStatement).Fields
	// fcs := stmt.(*jepl.SelectStatement).FunctionCalls()

	for i := 0; i < 10; i++ {
		js, _ := simplejson.NewJson([]byte(fmt.Sprintf(`{
            "uid": 1,
            "tcp": {"in_bytes":%d, "out_bytes": 20, "in_pkts": %d, "out_pkts": 2}
        }`, i*10, i)))
		switch res := jepl.Eval(cond, js.MustMap()).(type) {
		case bool:
			if res == true {
				stmt.(*jepl.SelectStatement).EvalFunctionCalls(js.MustMap())
			}
		default:
			fmt.Println("Select Where Condition parse error")
		}
	}
	ms := stmt.(*jepl.SelectStatement).EvalMetric()
	fmt.Println(ms)
}
