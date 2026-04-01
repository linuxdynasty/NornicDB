package buildinfo

import "testing"

func TestVersionHelpers(t *testing.T) {
	if Version() == "" {
		t.Fatal("Version() should not be empty")
	}
	if ProductVersion() != "v"+Version() {
		t.Fatalf("unexpected product version: %s", ProductVersion())
	}
	if ServerAnnouncement() != "NornicDB/"+Version() {
		t.Fatalf("unexpected server announcement: %s", ServerAnnouncement())
	}
}
