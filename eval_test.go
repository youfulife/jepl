package jepl_test

import (
	"encoding/json"
	"fmt"
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
	s := "select max(tcp.in_bytes), min(tcp.in_pkts), count(tcp.in_pkts), sum(tcp.in_pkts), avg(tcp.in_pkts) from packetbeat where uid = 1 group by tcp.src_ip, tcp.dst_ip"

	var docs []string
	for i := 0; i < 10; i++ {
		js := fmt.Sprintf(`{"uid": %d, "tcp": {"src_ip":%d, "dst_ip":%d, "in_bytes":%d, "out_bytes": 20, "in_pkts": %d, "out_pkts": 2}}`, i%3, i%2, i%3, i*10, i)
		docs = append(docs, js)
	}

	stmt, err := jepl.ParseStatement(s)
	if err != nil {
		panic(err)
	}
	// cond := stmt.(*jepl.SelectStatement).Condition
	// dimensions := stmt.(*jepl.SelectStatement).Dimensions
	// fields := stmt.(*jepl.SelectStatement).Fields
	// fcs := stmt.(*jepl.SelectStatement).FunctionCalls()

	stmts := stmt.(*jepl.SelectStatement).FlatStatByGroup(docs)
	for k, v := range stmts {
		for _, doc := range docs {
			switch res := jepl.Eval(v.Condition, &doc).(type) {
			case bool:
				if res == true {
					stmt.(*jepl.SelectStatement).EvalFunctionCalls(&doc)
				}
			default:
				fmt.Println("Select Where Condition parse error")
			}
		}
		ms := stmt.(*jepl.SelectStatement).EvalMetric()
		fmt.Println(k, ms)
	}
}

func BenchmarkEvalFunctionCalls(b *testing.B) {
	b.ReportAllocs()

	s := "select sum(_source.http.in_bytes+_source.http.out_bytes) AS total_bytes FROM packetbeat where _source.guid='4a859fff6e5c4521aab187eee1cfceb8'"

	stmt, err := jepl.ParseStatement(s)
	if err != nil {
		panic(err)
	}

	cond := stmt.(*jepl.SelectStatement).Condition
	// fields := stmt.(*jepl.SelectStatement).Fields
	// fcs := stmt.(*jepl.SelectStatement).FunctionCalls()

	for i := 0; i < b.N; i++ {
		js := `{
			"_index": "cc-cloudsensor-4a859fff6e5c4521aab187eee1cfceb8-2016.12.14",
			"_type": "http",
			"_id": "AVj-D8OzyUc7ekFJUXpB",
			"_score": null,
			"_timestamp": 1481731195827,
			"_source": {
				"@timestamp": "2016-12-14T23:59:55+08:00",
				"aggregate_count": 1,
				"appname": "cloudsensor",
				"dawn_ts0": 1481731195311000,
				"dawn_ts1": 1481731195311000,
				"device_id": "be8bb0ff-c73a-5ca6-afd8-871783d8b890",
				"fair_handle_latency_us": 105,
				"fair_ts0": 1481731195391680,
				"fair_ts1": 1481731195391785,
				"guid": "4a859fff6e5c4521aab187eee1cfceb8",
				"host": "list.com",
				"http": {
					"dst_ip": {
						"decimal": 2362426130,
						"dotted": "140.207.195.18",
						"isp": "联通",
						"latitude": "121.472644",
						"longtitude": "31.231706",
						"raw": 2362426130,
						"region": "上海"
					},
					"dst_port": 80,
					"host": "passport.bdimg.com",
					"http_method": 1,
					"https_flag": 0,
					"in_bytes": 305,
					"in_pkts": 1,
					"l4_protocol": "tcp",
					"latency_sec": 0,
					"latency_usec": 215779,
					"out_bytes": 675,
					"out_pkts": 1,
					"refer": "",
					"src_ip": {
						"decimal": 176189498,
						"dotted": "10.128.112.58",
						"isp": "",
						"latitude": "",
						"longtitude": "",
						"raw": 176189498,
						"region": ""
					},
					"src_port": 38558,
					"status_code": 200,
					"url": "/passApi/html/sdkloginconfig.html",
					"url_query": "",
					"user_agent": {
						"raw": ""
					},
					"xff": ""
				},
				"kafka": {
					"offset": 83107248,
					"partition": 0,
					"topic": "cloudsensor"
				},
				"probe": {
					"hostname": "list.com",
					"name": "cloudsensor"
				},
				"probe_ts": 1481731310,
				"topic": "cloudsensor",
				"type": "http"
			},
			"fields": {
				"@timestamp": [
				1481731195000
				]
			},
			"highlight": {
				"type": [
				"@kibana-highlighted-field@http@/kibana-highlighted-field@"
				]
			},
			"sort": [
			1481731195000
			]
		}`
		switch res := jepl.Eval(cond, &js).(type) {
		case bool:
			if res == true {
				stmt.(*jepl.SelectStatement).EvalFunctionCalls(&js)
			}
		default:
			fmt.Println("Select Where Condition parse error")
		}
	}
	fmt.Println(stmt.(*jepl.SelectStatement).EvalMetric())
}
