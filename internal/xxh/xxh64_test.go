package xxh

import (
	"math/rand"
	"strings"
	"testing"
)

// Reference vectors for XXH64 with a zero seed. These pin the implementation
// to the specification rather than to itself: a round-trip test would happily
// pass on a hash that is internally consistent and wrong.
func TestSum64ReferenceVectors(t *testing.T) {
	cases := []struct {
		in   string
		want uint64
	}{
		{"", 0xEF46DB3751D8E999},
		{"a", 0xD24EC4F1A98C6E5B},
		{"as", 0x1C330FB2D66BE179},
		{"asd", 0x631C37CE72A97393},
		{"asdf", 0x415872F599CEA71E},
		// 63 bytes: exercises the 32-byte stripe loop plus an 8/4/1-byte tail.
		{"Call me Ishmael. Some years ago--never mind how long precisely-", 0x02A2E85470D6FD96},
	}
	for _, tc := range cases {
		if got := Sum64([]byte(tc.in)); got != tc.want {
			t.Errorf("Sum64(%q) = %#016x, want %#016x", tc.in, got, tc.want)
		}
		if got := Sum64String(tc.in); got != tc.want {
			t.Errorf("Sum64String(%q) = %#016x, want %#016x", tc.in, got, tc.want)
		}
	}
}

// TestSum64AllLengths covers every tail path: the 32-byte stripe loop and the
// 8-, 4- and 1-byte remainder handling, at every boundary between them.
func TestSum64AllLengths(t *testing.T) {
	const maxLen = 200
	buf := make([]byte, maxLen)
	for i := range buf {
		buf[i] = byte(i * 7)
	}

	seen := make(map[uint64]int, maxLen)
	for n := 0; n <= maxLen; n++ {
		h := Sum64(buf[:n])
		if prev, dup := seen[h]; dup {
			t.Errorf("lengths %d and %d collide on %#016x", prev, n, h)
		}
		seen[h] = n
	}
}

func TestSum64SeedChangesDigest(t *testing.T) {
	in := []byte("node_cpu_seconds_total")
	a, b := Sum64Seed(in, 0), Sum64Seed(in, 1)
	if a == b {
		t.Errorf("seeds 0 and 1 produced the same digest %#016x", a)
	}
	if got := Sum64(in); got != a {
		t.Errorf("Sum64 = %#016x, Sum64Seed(_, 0) = %#016x; they must agree", got, a)
	}
}

// TestDigestMatchesOneShot is the property that matters for the streaming
// form: however the input is split across Write calls, the result must equal
// the one-shot digest. Splitting at the carry-buffer boundary is where a
// streaming hash normally breaks.
func TestDigestMatchesOneShot(t *testing.T) {
	rng := rand.New(rand.NewSource(11))
	data := make([]byte, 512)
	rng.Read(data)

	for _, n := range []int{0, 1, 7, 8, 31, 32, 33, 63, 64, 65, 127, 200, 512} {
		want := Sum64(data[:n])

		// Every fixed chunk size.
		for _, step := range []int{1, 3, 8, 16, 31, 32, 33, 64, 1000} {
			d := New()
			for i := 0; i < n; i += step {
				end := min(i+step, n)
				if _, err := d.Write(data[i:end]); err != nil {
					t.Fatalf("Write: %v", err)
				}
			}
			if got := d.Sum64(); got != want {
				t.Errorf("len=%d step=%d: got %#016x, want %#016x", n, step, got, want)
			}
		}

		// Random splits, which reach boundaries the fixed steps miss.
		for trial := 0; trial < 20; trial++ {
			d := New()
			for i := 0; i < n; {
				step := rng.Intn(40) + 1
				end := min(i+step, n)
				d.Write(data[i:end])
				i = end
			}
			if got := d.Sum64(); got != want {
				t.Errorf("len=%d random split: got %#016x, want %#016x", n, got, want)
			}
		}
	}
}

func TestDigestSum64IsRepeatable(t *testing.T) {
	// Sum64 must not consume state: callers hash a label set, read the sum,
	// then keep appending for a longer key.
	d := New()
	d.WriteString("cpu")
	first := d.Sum64()
	if second := d.Sum64(); first != second {
		t.Fatalf("Sum64 is not idempotent: %#016x then %#016x", first, second)
	}
	d.WriteString("_seconds")
	if got, want := d.Sum64(), Sum64String("cpu_seconds"); got != want {
		t.Errorf("after continued write: got %#016x, want %#016x", got, want)
	}
}

func TestDigestReset(t *testing.T) {
	d := New()
	d.WriteString(strings.Repeat("x", 100))
	d.Reset()
	d.WriteString("asdf")
	if got, want := d.Sum64(), uint64(0x415872F599CEA71E); got != want {
		t.Errorf("after Reset: got %#016x, want %#016x", got, want)
	}
}

func TestDigestWriteString(t *testing.T) {
	d := New()
	if n, err := d.WriteString("hello"); n != 5 || err != nil {
		t.Fatalf("WriteString = (%d, %v), want (5, nil)", n, err)
	}
	if got, want := d.Sum64(), Sum64String("hello"); got != want {
		t.Errorf("got %#016x, want %#016x", got, want)
	}
}

// TestSum64DistributionOverSeriesNames is a smoke test on the property that
// actually matters in use: label sets differing in one character must land in
// different buckets. A hash that fails this turns the head block's series map
// into a linked list.
func TestSum64DistributionOverSeriesNames(t *testing.T) {
	const (
		n       = 1 << 14
		buckets = 1 << 10
	)
	counts := make([]int, buckets)
	for i := 0; i < n; i++ {
		key := "node_cpu_seconds_total{cpu=\"" + string(rune('0'+i%10)) + "\",host=\"host-" + itoa(i) + "\"}"
		counts[Sum64String(key)%buckets]++
	}

	// With n/buckets = 16 expected per bucket, a uniform hash essentially
	// never exceeds 3x the mean. A broken one piles up immediately.
	const mean = n / buckets
	for i, c := range counts {
		if c > mean*3 {
			t.Errorf("bucket %d holds %d keys, expected around %d", i, c, mean)
		}
		if c == 0 {
			t.Errorf("bucket %d is empty, expected around %d", i, mean)
		}
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [20]byte
	p := len(b)
	for i > 0 {
		p--
		b[p] = byte('0' + i%10)
		i /= 10
	}
	return string(b[p:])
}

func BenchmarkSum64(b *testing.B) {
	for _, size := range []int{8, 32, 64, 256} {
		buf := make([]byte, size)
		rand.New(rand.NewSource(1)).Read(buf)
		b.Run(itoa(size)+"B", func(b *testing.B) {
			b.SetBytes(int64(size))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = Sum64(buf)
			}
		})
	}
}

func BenchmarkSum64String(b *testing.B) {
	const s = "node_cpu_seconds_total"
	b.SetBytes(int64(len(s)))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = Sum64String(s)
	}
}
