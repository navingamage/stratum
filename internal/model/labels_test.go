package model

import (
	"errors"
	"strings"
	"testing"
)

func TestFromStringsSorts(t *testing.T) {
	ls := FromStrings("zone", "eu", MetricName, "http_requests", "host", "web-1")
	want := []string{MetricName, "host", "zone"}
	if len(ls) != len(want) {
		t.Fatalf("got %d labels, want %d", len(ls), len(want))
	}
	for i, n := range want {
		if ls[i].Name != n {
			t.Errorf("label %d is %q, want %q", i, ls[i].Name, n)
		}
	}
}

func TestFromStringsRejectsOddArguments(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("FromStrings with an odd argument count did not panic")
		}
	}()
	FromStrings("a")
}

func TestNewRejectsDuplicates(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("New with duplicate names did not panic")
		}
	}()
	New(Label{"host", "a"}, Label{"host", "b"})
}

func TestGetAndHas(t *testing.T) {
	ls := FromStrings(MetricName, "cpu", "host", "web-1")
	if got := ls.Get("host"); got != "web-1" {
		t.Errorf("Get(host) = %q, want web-1", got)
	}
	if got := ls.Get("absent"); got != "" {
		t.Errorf("Get(absent) = %q, want empty", got)
	}
	if !ls.Has("host") || ls.Has("absent") {
		t.Error("Has disagrees with Get")
	}
}

func TestHashIsOrderIndependentAndStable(t *testing.T) {
	a := FromStrings("host", "web-1", MetricName, "cpu", "zone", "eu")
	b := FromStrings("zone", "eu", "host", "web-1", MetricName, "cpu")
	if a.Hash() != b.Hash() {
		t.Errorf("differently-ordered inputs hashed to %#x and %#x", a.Hash(), b.Hash())
	}

	// The head block maps a hash to a series ref and persists nothing else,
	// so the digest must not drift between calls.
	if a.Hash() != a.Hash() {
		t.Error("Hash is not deterministic")
	}
}

func TestHashDistinguishesSeparatorTricks(t *testing.T) {
	// The classic label-hashing bug: concatenating name and value without an
	// unambiguous separator makes {ab="c"} and {a="bc"} collide. The 0xff
	// separator cannot appear in valid UTF-8, so this is not expressible.
	a := FromStrings("ab", "c")
	b := FromStrings("a", "bc")
	if a.Hash() == b.Hash() {
		t.Errorf("{ab=%q} and {a=%q} both hashed to %#x", "c", "bc", a.Hash())
	}
}

func TestHashLargeLabelSetFallsBackToStreaming(t *testing.T) {
	// Past the 1KiB stack buffer, Hash switches to the streaming digest. Both
	// paths must agree on any set they can both encode, so build one that
	// crosses the boundary and check it is at least self-consistent and
	// distinct from a near neighbour.
	big := strings.Repeat("x", 600)
	a := FromStrings("a", big, "b", big)
	b := FromStrings("a", big, "b", big+"y")

	if a.Hash() != a.Copy().Hash() {
		t.Error("streaming path is not deterministic")
	}
	if a.Hash() == b.Hash() {
		t.Error("streaming path collided on differing label sets")
	}
}

func TestEqualAndCopy(t *testing.T) {
	a := FromStrings("host", "web-1", "zone", "eu")
	b := a.Copy()
	if !a.Equal(b) {
		t.Error("Copy is not Equal to its source")
	}

	b[0].Value = "web-2"
	if a.Equal(b) {
		t.Error("mutating the copy changed the original: Copy is shallow")
	}
	if a.Equal(FromStrings("host", "web-1")) {
		t.Error("sets of different lengths compared equal")
	}
}

func TestCompare(t *testing.T) {
	cases := []struct {
		a, b Labels
		want int // sign
	}{
		{FromStrings("a", "1"), FromStrings("a", "1"), 0},
		{FromStrings("a", "1"), FromStrings("a", "2"), -1},
		{FromStrings("a", "2"), FromStrings("a", "1"), +1},
		{FromStrings("a", "1"), FromStrings("b", "1"), -1},
		{FromStrings("a", "1"), FromStrings("a", "1", "b", "2"), -1},
		{FromStrings("a", "1", "b", "2"), FromStrings("a", "1"), +1},
		{EmptyLabels(), FromStrings("a", "1"), -1},
		{EmptyLabels(), EmptyLabels(), 0},
	}
	for _, tc := range cases {
		got := Compare(tc.a, tc.b)
		if sign(got) != tc.want {
			t.Errorf("Compare(%s, %s) = %d, want sign %d", tc.a, tc.b, got, tc.want)
		}
	}
}

func sign(n int) int {
	switch {
	case n < 0:
		return -1
	case n > 0:
		return 1
	}
	return 0
}

func TestString(t *testing.T) {
	cases := []struct {
		ls   Labels
		want string
	}{
		{EmptyLabels(), "{}"},
		{FromStrings(MetricName, "up"), "up"},
		{FromStrings(MetricName, "up", "host", "web-1"), `up{host="web-1"}`},
		{FromStrings("host", "web-1", "zone", "eu"), `{host="web-1", zone="eu"}`},
		{FromStrings(MetricName, "up", "note", `a "quoted" value`), `up{note="a \"quoted\" value"}`},
	}
	for _, tc := range cases {
		if got := tc.ls.String(); got != tc.want {
			t.Errorf("String() = %s, want %s", got, tc.want)
		}
	}
}

func TestIsValidLabelName(t *testing.T) {
	valid := []string{"a", "_", "__name__", "a1", "A_b_9", "_9"}
	invalid := []string{"", "1abc", "a-b", "a.b", "a b", "a\xff", "ä"}
	for _, n := range valid {
		if !IsValidLabelName(n) {
			t.Errorf("IsValidLabelName(%q) = false, want true", n)
		}
	}
	for _, n := range invalid {
		if IsValidLabelName(n) {
			t.Errorf("IsValidLabelName(%q) = true, want false", n)
		}
	}
}

func TestValidate(t *testing.T) {
	cases := []struct {
		name string
		ls   Labels
		want error
	}{
		{"ok", FromStrings("a", "1", "b", "2"), nil},
		{"empty set", EmptyLabels(), nil},
		{"empty value is allowed", FromStrings("a", ""), nil},
		{"empty name", Labels{{Name: "", Value: "x"}}, ErrEmptyLabelName},
		{"bad name", Labels{{Name: "a-b", Value: "x"}}, ErrInvalidLabelName},
		{"bad utf8", Labels{{Name: "a", Value: "\xff\xfe"}}, ErrInvalidUTF8},
		{"duplicate", Labels{{Name: "a", Value: "1"}, {Name: "a", Value: "2"}}, ErrDuplicateLabel},
		{"unsorted", Labels{{Name: "b", Value: "1"}, {Name: "a", Value: "2"}}, ErrUnsorted},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.ls.Validate()
			if tc.want == nil {
				if err != nil {
					t.Fatalf("Validate() = %v, want nil", err)
				}
				return
			}
			if !errors.Is(err, tc.want) {
				t.Errorf("Validate() = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestBytesIsUnambiguous(t *testing.T) {
	a := FromStrings("ab", "c").Bytes(nil)
	b := FromStrings("a", "bc").Bytes(nil)
	if string(a) == string(b) {
		t.Error("Bytes collided on label sets that differ")
	}
}

func TestMapRoundTrip(t *testing.T) {
	m := map[string]string{"host": "web-1", "zone": "eu"}
	ls := FromMap(m)
	got := ls.Map()
	if len(got) != len(m) {
		t.Fatalf("Map() has %d entries, want %d", len(got), len(m))
	}
	for k, v := range m {
		if got[k] != v {
			t.Errorf("Map()[%q] = %q, want %q", k, got[k], v)
		}
	}
}

func TestBuilder(t *testing.T) {
	base := FromStrings(MetricName, "cpu", "host", "web-1", "zone", "eu")

	t.Run("set and delete", func(t *testing.T) {
		got := NewBuilder(base).Set("host", "web-2").Del("zone").Labels()
		want := FromStrings(MetricName, "cpu", "host", "web-2")
		if !got.Equal(want) {
			t.Errorf("got %s, want %s", got, want)
		}
	})

	t.Run("base is not mutated", func(t *testing.T) {
		before := base.Copy()
		NewBuilder(base).Set("host", "other").Del(MetricName).Labels()
		if !base.Equal(before) {
			t.Errorf("base changed from %s to %s", before, base)
		}
	})

	t.Run("setting empty deletes", func(t *testing.T) {
		got := NewBuilder(base).Set("zone", "").Labels()
		if got.Has("zone") {
			t.Errorf("Set to empty left the label in place: %s", got)
		}
	})

	t.Run("overwriting a pending add", func(t *testing.T) {
		got := NewBuilder(base).Set("k", "1").Set("k", "2").Labels()
		if v := got.Get("k"); v != "2" {
			t.Errorf("Get(k) = %q, want 2", v)
		}
	})

	t.Run("keep", func(t *testing.T) {
		got := NewBuilder(base).Keep("host").Labels()
		want := FromStrings("host", "web-1")
		if !got.Equal(want) {
			t.Errorf("got %s, want %s", got, want)
		}
	})

	t.Run("no changes returns the base", func(t *testing.T) {
		got := NewBuilder(base).Labels()
		if !got.Equal(base) {
			t.Errorf("got %s, want %s", got, base)
		}
	})

	t.Run("result stays sorted", func(t *testing.T) {
		got := NewBuilder(base).Set("aaa", "1").Set("zzz", "2").Labels()
		if err := got.Validate(); err != nil {
			t.Errorf("builder produced an invalid set %s: %v", got, err)
		}
	})

	t.Run("empty base value is treated as absent", func(t *testing.T) {
		got := NewBuilder(Labels{{Name: "a", Value: ""}, {Name: "b", Value: "1"}}).Labels()
		if got.Has("a") {
			t.Errorf("empty-valued base label survived: %s", got)
		}
	})
}

func TestBuilderReset(t *testing.T) {
	b := NewBuilder(FromStrings("a", "1"))
	b.Set("x", "1")
	b.Reset(FromStrings("b", "2"))
	got := b.Labels()
	want := FromStrings("b", "2")
	if !got.Equal(want) {
		t.Errorf("after Reset got %s, want %s", got, want)
	}
}

// BenchmarkLabelsHash guards the claim that hashing a typical label set does
// not allocate. Ingest calls this once per sample, so a regression here shows
// up directly as GC pressure under load.
func BenchmarkLabelsHash(b *testing.B) {
	ls := FromStrings(
		MetricName, "node_cpu_seconds_total",
		"cpu", "12",
		"host", "web-042.eu-west-1.internal",
		"instance", "10.24.11.98:9100",
		"job", "node-exporter",
		"mode", "system",
		"zone", "eu-west-1c",
	)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = ls.Hash()
	}
}

func BenchmarkLabelsGet(b *testing.B) {
	ls := FromStrings(MetricName, "cpu", "host", "web-1", "job", "node", "zone", "eu")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = ls.Get("zone")
	}
}
