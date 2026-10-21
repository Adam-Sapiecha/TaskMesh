package handlers

import (
	"context"
	"encoding/json"
	"testing"
)

func TestBuiltinResults(t *testing.T) {
	r := Builtins()
	for _, test := range []struct{ typ, payload, want string }{
		{"text.analyze", `{"text":"Zażółć gęślą\nworld"}`, `{"characters":18,"lines":2,"words":3}`},
		{"report.generate", `{"values":[2,4,9]}`, `{"count":3,"max":9,"mean":5,"min":2,"sum":15}`},
		{"benchmark.compute", `{"seed":"abc","rounds":1}`, `{"rounds":1,"sha256":"ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"}`},
	} {
		t.Run(test.typ, func(t *testing.T) {
			raw := json.RawMessage(test.payload)
			if err := r.Validate(test.typ, raw); err != nil {
				t.Fatal(err)
			}
			got, err := r[test.typ].Execute(context.Background(), raw)
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != test.want {
				t.Fatalf("got %s want %s", got, test.want)
			}
		})
	}
}
func TestRejectUnsafeAndMalformedPayloads(t *testing.T) {
	r := Builtins()
	for _, test := range []struct{ typ, payload string }{
		{"shell.exec", `{"command":"whoami"}`}, {"text.analyze", `{"text":"x","unexpected":1}`}, {"text.analyze", `null`}, {"report.generate", `{"values":[]}`}, {"benchmark.compute", `{"rounds":100001}`}, {"report.generate", `{"values":[1e100]}`}, {"text.analyze", `{"text":"x"} {}`},
	} {
		if err := r.Validate(test.typ, json.RawMessage(test.payload)); err == nil {
			t.Fatalf("accepted %s %s", test.typ, test.payload)
		}
	}
}
func TestCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Builtins()["benchmark.compute"].Execute(ctx, json.RawMessage(`{"seed":"test","rounds":100}`)); err == nil {
		t.Fatal("cancelled computation succeeded")
	}
}
