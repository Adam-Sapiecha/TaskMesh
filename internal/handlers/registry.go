package handlers

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"unicode/utf8"
)

type Handler struct {
	Validate func(json.RawMessage) error
	Execute  func(context.Context, json.RawMessage) (json.RawMessage, error)
}
type Registry map[string]Handler

// Decode rejects misspelled fields and trailing JSON, rather than silently ignoring them.
func Decode(raw []byte, target any) error {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(target); err != nil {
		return err
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return errors.New("expected one JSON object")
	}
	return nil
}
func Builtins() Registry {
	type textInput struct {
		Text string `json:"text"`
	}
	type reportInput struct {
		Values []float64 `json:"values"`
	}
	type computeInput struct {
		Seed   string `json:"seed"`
		Rounds int    `json:"rounds"`
	}
	textValidate := func(raw json.RawMessage) error {
		var p textInput
		if err := Decode(raw, &p); err != nil {
			return err
		}
		if len(p.Text) == 0 || len(p.Text) > 32768 {
			return errors.New("text must contain 1..32768 bytes")
		}
		return nil
	}
	reportValidate := func(raw json.RawMessage) error {
		var p reportInput
		if err := Decode(raw, &p); err != nil {
			return err
		}
		if len(p.Values) == 0 || len(p.Values) > 1000 {
			return errors.New("values must contain 1..1000 numbers")
		}
		for _, v := range p.Values {
			if math.IsNaN(v) || math.IsInf(v, 0) || math.Abs(v) > 1e12 {
				return errors.New("numbers must be finite and within +/-1e12")
			}
		}
		return nil
	}
	computeValidate := func(raw json.RawMessage) error {
		var p computeInput
		if err := Decode(raw, &p); err != nil {
			return err
		}
		if len(p.Seed) > 4096 || p.Rounds < 1 || p.Rounds > 100000 {
			return errors.New("seed <=4096 bytes and rounds 1..100000 required")
		}
		return nil
	}
	return Registry{
		"text.analyze": {textValidate, func(ctx context.Context, raw json.RawMessage) (json.RawMessage, error) {
			var p textInput
			_ = Decode(raw, &p)
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			return json.Marshal(map[string]any{"words": len(strings.Fields(p.Text)), "characters": utf8.RuneCountInString(p.Text), "lines": strings.Count(p.Text, "\n") + 1})
		}},
		"report.generate": {reportValidate, func(ctx context.Context, raw json.RawMessage) (json.RawMessage, error) {
			var p reportInput
			_ = Decode(raw, &p)
			sum, min, max := 0.0, p.Values[0], p.Values[0]
			for _, v := range p.Values {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				sum += v
				min = math.Min(min, v)
				max = math.Max(max, v)
			}
			return json.Marshal(map[string]any{"count": len(p.Values), "sum": sum, "mean": sum / float64(len(p.Values)), "min": min, "max": max})
		}},
		"benchmark.compute": {computeValidate, func(ctx context.Context, raw json.RawMessage) (json.RawMessage, error) {
			var p computeInput
			_ = Decode(raw, &p)
			b := []byte(p.Seed)
			for i := 0; i < p.Rounds; i++ {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				s := sha256.Sum256(b)
				b = s[:]
			}
			return json.Marshal(map[string]any{"sha256": hex.EncodeToString(b), "rounds": p.Rounds})
		}},
	}
}
func (r Registry) Validate(typ string, raw json.RawMessage) error {
	h, ok := r[typ]
	if !ok {
		return fmt.Errorf("unregistered task type %q", typ)
	}
	if len(raw) == 0 || raw[0] != '{' {
		return errors.New("payload must be an object")
	}
	return h.Validate(raw)
}
func (r Registry) Types() []string {
	out := make([]string, 0, len(r))
	for k := range r {
		out = append(out, k)
	}
	return out
}
