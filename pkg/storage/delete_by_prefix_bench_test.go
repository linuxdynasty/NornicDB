package storage

import (
	"fmt"
	"testing"
)

func BenchmarkBadgerEngine_DeleteByPrefix(b *testing.B) {
	const (
		dbPrefix = "benchdb:"
		nodes    = 5000
	)

	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		engine := NewMemoryEngine()

		for n := 0; n < nodes; n++ {
			_, err := engine.CreateNode(&Node{
				ID:     NodeID(fmt.Sprintf("%sn%d", dbPrefix, n)),
				Labels: []string{"Person"},
				Properties: map[string]any{
					"i": n,
				},
			})
			if err != nil {
				b.Fatal(err)
			}
		}

		for e := 0; e < nodes-1; e++ {
			err := engine.CreateEdge(&Edge{
				ID:        EdgeID(fmt.Sprintf("%se%d", dbPrefix, e)),
				StartNode: NodeID(fmt.Sprintf("%sn%d", dbPrefix, e)),
				EndNode:   NodeID(fmt.Sprintf("%sn%d", dbPrefix, e+1)),
				Type:      "KNOWS",
			})
			if err != nil {
				b.Fatal(err)
			}
		}

		// Populate caches so DROP behavior is realistic.
		_, _ = engine.GetNodesByLabel("Person")
		_, _ = engine.GetEdgesByType("KNOWS")

		b.StartTimer()
		_, _, err := engine.DeleteByPrefix(dbPrefix)
		if err != nil {
			b.Fatal(err)
		}
		b.StopTimer()

		_ = engine.Close()
	}
}
