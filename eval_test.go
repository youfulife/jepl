package jepl_test

import (
	"fmt"
	"github.com/bitly/go-simplejson"
	"github.com/chenyoufu/jepl"
	"testing"
)

func TestEvalQuery(t *testing.T) {
	s := "select avg(tcp.in_pkts)/avg(tcp.out_pkts), sum(tcp.in_bytes + tcp.out_bytes) AS tcp_total_bytes from packetbeat where uid = 1"

	stmt, err := jepl.ParseStatement(s)
	if err != nil {
		panic(err)
	}
	// cond := stmt.(*jepl.SelectStatement).Condition
	// fields := stmt.(*jepl.SelectStatement).Fields
	// fcs := stmt.(*jepl.SelectStatement).FunctionCalls()

	for i := 0; i < 2; i++ {
		js, _ := simplejson.NewJson([]byte(`{
            "uid": 1,
            "tcp": {"in_bytes":10, "out_bytes": 20, "in_pkts": 50, "out_pkts": 20}
        }`))
		stmt.(*jepl.SelectStatement).EvalFunctionCalls(js.MustMap())
	}
	ms := stmt.(*jepl.SelectStatement).EvalMetric()
	fmt.Println(ms)
}
