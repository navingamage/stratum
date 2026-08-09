package compact

import (
	"fmt"
	"log/slog"
	"path/filepath"
	"sort"
	"time"

	"github.com/navingamage/stratum/internal/block"
	"github.com/navingamage/stratum/internal/memtable"
)

// DefaultRanges is the ladder of block durations compaction aims for: two
// hours, then 3x at each level.
//
// The ratio is a compromise. A larger factor means fewer, bigger blocks and so
// fewer index lookups per query, but every compaction rewrites the whole
// output, so the write amplification of merging n blocks into one is paid
// again at the next level up. Three keeps the tree shallow - a month of data
// is five levels - without any single compaction rewriting an unreasonable
// amount at once.
var DefaultRanges = []int64{
	2 * 60 * 60 * 1000,   // 2h
	6 * 60 * 60 * 1000,   // 6h
	18 * 60 * 60 * 1000,  // 18h
	54 * 60 * 60 * 1000,  // 2d6h
	162 * 60 * 60 * 1000, // 6d18h
	486 * 60 * 60 * 1000, // 20d6h
}

// Options configures a compactor.
type Options struct {
	// Ranges is the block duration ladder. Zero selects DefaultRanges.
	Ranges []int64

	// SamplesPerChunk is the chunk size for re-encoded output. Zero selects
	// the head's default.
	SamplesPerChunk int

	// Retention drops blocks entirely older than this. Zero disables it.
	Retention time.Duration

	// Logger receives progress. Zero uses the default logger.
	Logger *slog.Logger
}

func (o Options) withDefaults() Options {
	if len(o.Ranges) == 0 {
		o.Ranges = DefaultRanges
	}
	if o.SamplesPerChunk <= 0 {
		o.SamplesPerChunk = memtable.DefaultSamplesPerChunk
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	return o
}

// Compactor plans and performs block compactions.
type Compactor struct {
	dir  string
	opts Options
}

// New returns a compactor over a data directory.
func New(dir string, opts Options) *Compactor {
	return &Compactor{dir: dir, opts: opts.withDefaults()}
}

// Plan is a set of blocks to merge into one.
type Plan struct {
	// Metas are the inputs, oldest first.
	Metas []*block.Meta

	// Range is the target duration this plan compacts into, for logging.
	Range int64
}

// Bounds returns the time range the plan covers.
func (p *Plan) Bounds() (mint, maxt int64) {
	mint, maxt = p.Metas[0].MinTime, p.Metas[0].MaxTime
	for _, m := range p.Metas[1:] {
		if m.MinTime < mint {
			mint = m.MinTime
		}
		if m.MaxTime > maxt {
			maxt = m.MaxTime
		}
	}
	return mint, maxt
}

// Plan selects the next group of blocks to compact, or nil if none is due.
//
// Only one group is returned per call. Compaction is a background activity
// competing with ingest for disk bandwidth, and doing one merge then
// re-planning keeps the decision current - a group chosen ten minutes ago may
// no longer be the most useful one.
func (c *Compactor) Plan(metas []*block.Meta) *Plan {
	if len(metas) < 2 {
		return nil
	}

	sorted := append([]*block.Meta(nil), metas...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].MinTime < sorted[j].MinTime })

	// Overlapping blocks are compacted before anything else. They should not
	// arise in normal operation, but if they do, every query over the
	// overlap pays for reading both - and a merge is the only thing that
	// fixes it.
	if p := planOverlapping(sorted); p != nil {
		return p
	}

	for _, r := range c.opts.Ranges {
		if p := planRange(sorted, r); p != nil {
			return p
		}
	}
	return nil
}

func planOverlapping(sorted []*block.Meta) *Plan {
	for i := 1; i < len(sorted); i++ {
		if sorted[i].MinTime <= sorted[i-1].MaxTime {
			group := []*block.Meta{sorted[i-1], sorted[i]}
			// Absorb any further blocks that also overlap the running range.
			maxt := max(sorted[i-1].MaxTime, sorted[i].MaxTime)
			for j := i + 1; j < len(sorted) && sorted[j].MinTime <= maxt; j++ {
				group = append(group, sorted[j])
				maxt = max(maxt, sorted[j].MaxTime)
			}
			return &Plan{Metas: group}
		}
	}
	return nil
}

// planRange groups blocks that fall inside the same aligned window of length
// r. Alignment is on absolute epoch time rather than relative to the oldest
// block, so the grouping a block belongs to never changes as blocks come and
// go - which is what keeps compaction from repeatedly rewriting the same data
// into differently-shaped blocks.
func planRange(sorted []*block.Meta, r int64) *Plan {
	var (
		group   []*block.Meta
		current int64
		haveCur bool
	)

	flush := func() *Plan {
		if len(group) < 2 {
			return nil
		}
		// Only compact once the window is complete. Merging a window that is
		// still being written to would produce a block that immediately
		// overlaps the next flush.
		total := int64(0)
		for _, m := range group {
			total += m.MaxTime - m.MinTime
		}
		if total < r/2 {
			return nil
		}
		return &Plan{Metas: group, Range: r}
	}

	for _, m := range sorted {
		// A block already at or above the target size is not a candidate.
		if m.MaxTime-m.MinTime >= r {
			if p := flush(); p != nil {
				return p
			}
			group, haveCur = nil, false
			continue
		}

		w := (m.MinTime / r) * r
		if haveCur && w != current {
			if p := flush(); p != nil {
				return p
			}
			group = nil
		}
		current, haveCur = w, true
		group = append(group, m)
	}
	return flush()
}

// Compact merges the blocks named by a plan into a new one and deletes the
// inputs.
//
// The output is written and published before any input is removed. A crash
// between the two leaves the data present twice, which queries deduplicate
// and the next compaction cleans up - whereas deleting first would lose it.
func (c *Compactor) Compact(p *Plan) (*block.Meta, error) {
	if len(p.Metas) == 0 {
		return nil, nil
	}
	start := time.Now()

	dirs := make([]string, len(p.Metas))
	for i, m := range p.Metas {
		dirs[i] = blockDir(c.dir, m)
	}

	opened := make([]*block.Block, 0, len(dirs))
	defer func() {
		for _, b := range opened {
			b.Close()
		}
	}()

	sources := make([]block.SeriesSource, 0, len(dirs))
	for _, d := range dirs {
		b, err := block.Open(d)
		if err != nil {
			return nil, fmt.Errorf("compact: opening %s: %w", d, err)
		}
		opened = append(opened, b)
		sources = append(sources, NewBlockSource(b))
	}

	meta, err := block.Write(c.dir,
		NewMergeSource(c.opts.SamplesPerChunk, sources...),
		compactionFor(p.Metas))
	if err != nil {
		return nil, err
	}

	// Release the mappings before deleting the files they point at.
	for _, b := range opened {
		b.Close()
	}
	opened = nil

	for _, d := range dirs {
		if err := block.Delete(d); err != nil {
			// The output already exists, so a failure here leaves duplicate
			// data rather than missing data. Report it and let the next
			// compaction retry.
			return meta, fmt.Errorf("compact: removing the compacted input %s: %w", d, err)
		}
	}

	if meta != nil {
		c.opts.Logger.Info("compacted blocks",
			"inputs", len(dirs),
			"output", meta.ID.String(),
			"level", meta.Compaction.Level,
			"series", meta.Stats.NumSeries,
			"samples", meta.Stats.NumSamples,
			"elapsed", time.Since(start))
	}
	return meta, nil
}

// compactionFor derives the lineage of a compaction output from its inputs.
func compactionFor(metas []*block.Meta) block.Compaction {
	out := block.Compaction{Level: 1}
	for _, m := range metas {
		if m.Compaction.Level >= out.Level {
			out.Level = m.Compaction.Level + 1
		}
		out.Parents = append(out.Parents, m.ID)

		// Sources are the level-1 ancestors. Carrying them forward is what
		// lets a restart tell an already-absorbed input from a fresh one.
		if len(m.Compaction.Sources) > 0 {
			out.Sources = append(out.Sources, m.Compaction.Sources...)
		} else {
			out.Sources = append(out.Sources, m.ID)
		}
	}
	sort.Slice(out.Sources, func(i, j int) bool { return out.Sources[i].Compare(out.Sources[j]) < 0 })
	return out
}

// Flush writes the head's samples in [mint, maxt] into a new block.
func (c *Compactor) Flush(h *memtable.Head, mint, maxt int64) (*block.Meta, error) {
	start := time.Now()

	src, err := NewHeadSource(h, mint, maxt)
	if err != nil {
		return nil, err
	}
	meta, err := block.Write(c.dir, src, block.Compaction{Level: 1})
	if err != nil {
		return nil, fmt.Errorf("compact: flushing the head: %w", err)
	}
	if meta == nil {
		return nil, nil
	}

	c.opts.Logger.Info("flushed the head to a block",
		"block", meta.ID.String(),
		"series", meta.Stats.NumSeries,
		"samples", meta.Stats.NumSamples,
		"minTime", meta.MinTime,
		"maxTime", meta.MaxTime,
		"elapsed", time.Since(start))
	return meta, nil
}

// ApplyRetention deletes blocks that lie entirely outside the retention
// window, and reports how many went.
//
// Whole blocks only. Trimming a block that straddles the boundary would mean
// rewriting it, and it will fall out of the window entirely soon enough; the
// cost of holding it a little longer is bounded by one block duration.
func (c *Compactor) ApplyRetention(metas []*block.Meta, now time.Time) (int, error) {
	if c.opts.Retention <= 0 {
		return 0, nil
	}
	cutoff := now.Add(-c.opts.Retention).UnixMilli()

	deleted := 0
	for _, m := range metas {
		if m.MaxTime >= cutoff {
			continue
		}
		dir := blockDir(c.dir, m)
		if err := block.Delete(dir); err != nil {
			return deleted, fmt.Errorf("compact: applying retention to %s: %w", dir, err)
		}
		c.opts.Logger.Info("deleted a block past the retention window",
			"block", m.ID.String(),
			"maxTime", m.MaxTime,
			"cutoff", cutoff)
		deleted++
	}
	return deleted, nil
}

func blockDir(root string, m *block.Meta) string {
	return filepath.Join(root, m.ID.String())
}
