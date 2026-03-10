package search

import "testing"

func TestResolveCompressedANNProfile_InactiveWithoutVectorStore(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_ANN_QUALITY", "compressed")
	p := ResolveCompressedANNProfile(10000, 384, false)
	if p.Active {
		t.Fatalf("expected inactive profile")
	}
	if len(p.Diagnostics) == 0 {
		t.Fatalf("expected diagnostics")
	}
}

func TestResolveCompressedANNProfile_ActiveWithValidSettings(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_ANN_QUALITY", "compressed")
	t.Setenv("NORNICDB_VECTOR_IVF_LISTS", "256")
	t.Setenv("NORNICDB_VECTOR_PQ_SEGMENTS", "16")
	t.Setenv("NORNICDB_VECTOR_PQ_BITS", "8")
	p := ResolveCompressedANNProfile(50000, 384, true)
	if !p.Active {
		t.Fatalf("expected active profile, diagnostics=%v", p.Diagnostics)
	}
	if p.IVFLists != 256 || p.PQSegments != 16 || p.PQBits != 8 {
		t.Fatalf("unexpected profile values: %+v", p)
	}
}

func TestResolveCompressedANNProfile_NonCompressedQuality(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_ANN_QUALITY", "balanced")
	p := ResolveCompressedANNProfile(10000, 384, true)
	if p.Active {
		t.Fatalf("expected inactive profile for non-compressed quality")
	}
	if len(p.Diagnostics) == 0 || p.Diagnostics[0].Code != "quality_not_compressed" {
		t.Fatalf("expected quality_not_compressed diagnostic, got: %+v", p.Diagnostics)
	}
}

func TestResolveCompressedANNProfile_DiagnosticsAndHelpers(t *testing.T) {
	t.Setenv("NORNICDB_VECTOR_ANN_QUALITY", "compressed")
	t.Setenv("NORNICDB_VECTOR_PQ_SEGMENTS", "7")                  // not divisible for 64 dims
	t.Setenv("NORNICDB_VECTOR_IVF_LISTS", "4096")                 // requires >= 16384 vectors
	t.Setenv("NORNICDB_VECTOR_IVFPQ_TRAINING_SAMPLE_MAX", "1000") // below IVF lists

	p := ResolveCompressedANNProfile(100, 64, true)
	if p.Active {
		t.Fatalf("expected inactive profile due to diagnostics")
	}
	found := map[string]bool{}
	for _, d := range p.Diagnostics {
		found[d.Code] = true
	}
	if !found["invalid_segments"] {
		t.Fatalf("expected invalid_segments diagnostic, got: %+v", p.Diagnostics)
	}
	if !found["insufficient_vectors"] {
		t.Fatalf("expected insufficient_vectors diagnostic, got: %+v", p.Diagnostics)
	}
	if !found["training_sample_too_small"] {
		t.Fatalf("expected training_sample_too_small diagnostic, got: %+v", p.Diagnostics)
	}

	if got := clampInt(1, 3, 9); got != 3 {
		t.Fatalf("clampInt min bound mismatch: %d", got)
	}
	if got := clampInt(12, 3, 9); got != 9 {
		t.Fatalf("clampInt max bound mismatch: %d", got)
	}
	if got := clampInt(6, 3, 9); got != 6 {
		t.Fatalf("clampInt in-range mismatch: %d", got)
	}

	if got := autoIVFLists(0); got != 64 {
		t.Fatalf("autoIVFLists(0) mismatch: %d", got)
	}
	if got := autoIVFLists(2000000); got > 8192 || got < 16 {
		t.Fatalf("autoIVFLists clamp mismatch: %d", got)
	}
	if got := maxInt(5, 9); got != 9 {
		t.Fatalf("maxInt mismatch: %d", got)
	}
}
