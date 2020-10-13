package trace

import (
	"github.com/opentracing/opentracing-go"
	"testing"
	"time"
)

func TestDelaySpan_Finish(t *testing.T) {
	type fields struct {
		Span       opentracing.Span
		FinishTime time.Time
	}
	tests := []struct {
		name   string
		fields fields
	}{
		{
			name: "panic",
			fields: struct {
				Span       opentracing.Span
				FinishTime time.Time
			}{
				Span: nil,
				FinishTime: time.Time{},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Fatalf("don't cause a panic")
				}
			}()
			d := &DelaySpan{
				Span:       tt.fields.Span,
				FinishTime: tt.fields.FinishTime,
			}
			d.Finish()
		})
	}
}