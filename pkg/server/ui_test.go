package server

import (
	"strings"
	"testing"
)

func TestNormalizeUIBasePath(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		in   string
		want string
	}{
		{name: "empty", in: "", want: ""},
		{name: "root", in: "/", want: ""},
		{name: "valid_no_slash", in: "nornic-db", want: "/nornic-db"},
		{name: "valid_with_slash", in: "/nornic-db", want: "/nornic-db"},
		{name: "trim_trailing", in: "/nornic-db/", want: "/nornic-db"},
		{name: "collapse_double_slash", in: "/nornic-db//sub", want: "/nornic-db/sub"},
		{name: "reject_dotdot", in: "/nornic-db/../x", want: ""},
		{name: "reject_quote", in: `/nornic-db" onload="alert(1)`, want: ""},
		{name: "reject_angle", in: "/nornic-db<script>", want: ""},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := normalizeUIBasePath(tc.in)
			if got != tc.want {
				t.Fatalf("normalizeUIBasePath(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestRewriteIndexHTMLBasePath_RejectsUnsafeHeaderInput(t *testing.T) {
	t.Parallel()

	index := []byte(`<html><head><script src="/assets/app.js"></script><link href="/assets/app.css"></head></html>`)
	unsafeBase := `/nornic-db" onload="alert(1)`
	out := string(rewriteIndexHTMLBasePath(index, unsafeBase))

	if strings.Contains(out, "onload=") {
		t.Fatalf("unsafe attribute injection was reflected: %s", out)
	}
	if strings.Contains(out, unsafeBase) {
		t.Fatalf("unsafe base path was reflected: %s", out)
	}
	if !strings.Contains(out, `src="/assets/app.js"`) {
		t.Fatalf("expected original asset path preserved for unsafe base path: %s", out)
	}
}
