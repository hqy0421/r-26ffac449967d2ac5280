package flatjsonl

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"

	xsync "github.com/puzpuzpuz/xsync/v4"
)

// frozenPlanMode reports whether the run writes against an existing frozen
// plan file. It works before PrepareKeys loads the plan.
func (p *Processor) frozenPlanMode() bool {
	if p.f.ParquetPlan == "" {
		return false
	}

	_, err := os.Stat(p.f.ParquetPlan)

	return err == nil
}

// prepareFrozenPlanKeys either scans the inputs to create and save a frozen
// Parquet column plan, or loads an existing plan and rebuilds the processor
// key structures strictly from it.
func (p *Processor) prepareFrozenPlanKeys() error {
	if p.f.Parquet == "" {
		return errors.New("-parquet-plan requires -parquet output")
	}

	if len(p.cfg.Transpose) > 0 {
		return errors.New("-parquet-plan cannot be combined with transpose config: transposed tables have hour-dependent columns")
	}

	if _, err := os.Stat(p.f.ParquetPlan); err == nil {
		return p.loadFrozenPlanKeys()
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("stat frozen plan %s: %w", p.f.ParquetPlan, err)
	}

	// Plan mode: run the regular key scan to discover all columns.
	if len(p.includeRegex) == 0 && len(p.cfg.IncludeKeys) > 0 {
		p.iterateIncludeKeys()
	} else {
		p.resetKeyScanProgress()

		if err := p.scanAvailableKeys(); err != nil {
			return err
		}

		p.Log(fmt.Sprintf("lines: %d, keys: %d", p.pr.Lines(), len(p.includeKeys)))
		p.totalLines = int(p.pr.Lines())
	}

	p.prepareKeys()

	if len(p.keys) == 0 {
		return fmt.Errorf("no columns discovered from planning inputs, frozen plan %s not written", p.f.ParquetPlan)
	}

	if err := p.appendUnknownPlanColumns(); err != nil {
		return err
	}

	plan := FrozenPlan{
		Version:       FrozenPlanVersion,
		UnknownFields: p.f.UnknownFields,
		AddSequence:   p.f.AddSequence,
		Columns:       make([]FrozenPlanColumn, 0, len(p.keys)),
	}

	// Reverse index from value position to the original include key, so that
	// constant columns (whose flKey.original is empty after prepareKeys) keep
	// their "const:..." identity in the plan.
	origByIndex := make(map[int]string, len(p.includeKeys))
	for name, idx := range p.includeKeys {
		origByIndex[idx] = name
	}

	for i, k := range p.keys {
		col := FrozenPlanColumn{
			Name:     k.original,
			Replaced: k.replaced,
			Type:     k.t,
		}

		if col.Name == "" {
			col.Name = origByIndex[i]
		}

		// Constant columns are filled with string values at write time.
		if col.Type == TypeAbsent {
			col.Type = TypeString
		}

		plan.Columns = append(plan.Columns, col)
	}

	if err := SaveFrozenPlan(p.f.ParquetPlan, plan); err != nil {
		return err
	}

	p.frozenPlan = &plan
	p.Log(fmt.Sprintf("frozen parquet plan written to %s, %d columns", p.f.ParquetPlan, len(plan.Columns)))

	return nil
}

// restoreConstValues rebuilds the constant column values for a plan loaded
// from disk, since const columns are produced by configuration rather than
// read from input lines.
func (p *Processor) restoreConstValues() {
	for i, col := range p.frozenPlan.Columns {
		if strings.HasPrefix(col.Name, "const:") {
			p.constVals[i] = strings.TrimPrefix(col.Name, "const:")
		}
	}
}

// appendUnknownPlanColumns adds the reserved unknown-field columns at the end
// of the plan, after all data columns, so the plan column order stays stable
// for later hourly runs.
func (p *Processor) appendUnknownPlanColumns() error {
	if !p.f.UnknownFields {
		return nil
	}

	for _, reserved := range []string{FrozenUnknownFieldsColumn, FrozenUnknownSourceColumn} {
		if _, ok := p.includeKeys[reserved]; ok {
			return fmt.Errorf("reserved frozen plan column %q collides with an input key, rename it or disable -unknown-fields", reserved)
		}
	}

	idx := len(p.keys)

	fieldsKey := flKey{
		path:      strings.Split(strings.TrimPrefix(FrozenUnknownFieldsColumn, "."), "."),
		original:  FrozenUnknownFieldsColumn,
		canonical: p.ck(FrozenUnknownFieldsColumn),
		replaced:  p.prepareKey(FrozenUnknownFieldsColumn),
		t:         TypeString,
	}
	sourceKey := flKey{
		path:      strings.Split(strings.TrimPrefix(FrozenUnknownSourceColumn, "."), "."),
		original:  FrozenUnknownSourceColumn,
		canonical: p.ck(FrozenUnknownSourceColumn),
		replaced:  p.prepareKey(FrozenUnknownSourceColumn),
		t:         TypeString,
	}

	p.keys = append(p.keys, fieldsKey, sourceKey)
	p.includeKeys[fieldsKey.original] = idx
	p.includeKeys[sourceKey.original] = idx + 1
	p.unknownFieldsIndex = idx
	p.unknownSourceIndex = idx + 1

	return nil
}

// loadFrozenPlanKeys rebuilds processor key structures from an existing plan
// without scanning the hourly input. The same structures drive every writer,
// so all outputs of the partition share one column order and type set.
func (p *Processor) loadFrozenPlanKeys() error {
	plan, err := LoadFrozenPlan(p.f.ParquetPlan)
	if err != nil {
		return err
	}

	if plan.UnknownFields && !p.f.UnknownFields {
		return fmt.Errorf("frozen plan %s requires -unknown-fields", p.f.ParquetPlan)
	}

	if !plan.UnknownFields && p.f.UnknownFields {
		return fmt.Errorf("frozen plan %s was created without -unknown-fields, drop the flag or recreate the plan", p.f.ParquetPlan)
	}

	if plan.AddSequence != p.f.AddSequence {
		return fmt.Errorf("frozen plan %s was created with add-sequence=%v, rerun with the same -add-sequence setting",
			p.f.ParquetPlan, plan.AddSequence)
	}

	p.frozenPlan = &plan
	p.frozenPlanWrite = true
	p.f.AddSequence = plan.AddSequence

	h := newHasher()
	p.keys = make([]flKey, 0, len(plan.Columns))
	p.includeKeys = make(map[string]int, len(plan.Columns))
	p.flKeysList = make([]string, 0, len(plan.Columns))
	p.canonicalKeys = make(map[string]flKey, len(plan.Columns))
	p.constVals = map[int]string{}
	p.flKeys = xsync.NewMap[uint64, flKey]()

	for i, col := range plan.Columns {
		path := strings.Split(strings.TrimPrefix(col.Name, "."), ".")

		k := flKey{
			path:      path,
			original:  col.Name,
			canonical: p.ck(col.Name),
			replaced:  col.Replaced,
			t:         col.Type,
		}
		k.UpdateType(col.Type)

		// Constant columns are not walked from the input, their values are
		// injected in writeIterator.complete via constVals.
		if !strings.HasPrefix(col.Name, "const:") {
			p.flKeys.Store(h.hashBytes([]byte(col.Name)), k)
		}

		switch col.Name { //nolint:exhaustive
		case FrozenUnknownFieldsColumn:
			p.unknownFieldsIndex = i
		case FrozenUnknownSourceColumn:
			p.unknownSourceIndex = i
		}

		p.keys = append(p.keys, k)
		p.includeKeys[col.Name] = i
		p.flKeysList = append(p.flKeysList, col.Name)
		p.canonicalKeys[k.canonical] = k
	}

	if p.f.UnknownFields && (p.unknownFieldsIndex < 0 || p.unknownSourceIndex < 0) {
		return fmt.Errorf("frozen plan %s is missing reserved unknown-field columns", p.f.ParquetPlan)
	}

	p.restoreConstValues()
	p.Log(fmt.Sprintf("loaded frozen parquet plan %s, %d columns", p.f.ParquetPlan, len(plan.Columns)))

	return nil
}

// unknownFieldEntry is one flat field captured because it is not part of the
// frozen plan.
type unknownFieldEntry struct {
	Path  string          `json:"path"`
	Value json.RawMessage `json:"value"`
}

// sortedUnknownFields renders captured unknown fields as a stable JSON object,
// keyed by flat path.
func sortedUnknownFields(entries map[string]unknownFieldEntry) (string, error) {
	paths := make([]string, 0, len(entries))
	for path := range entries {
		paths = append(paths, path)
	}

	sort.Strings(paths)

	var sb strings.Builder

	sb.WriteByte('{')

	for i, path := range paths {
		if i > 0 {
			sb.WriteByte(',')
		}

		name, err := json.Marshal(path)
		if err != nil {
			return "", fmt.Errorf("marshal unknown field path: %w", err)
		}

		sb.Write(name)
		sb.WriteByte(':')
		sb.Write(entries[path].Value)
	}

	sb.WriteByte('}')

	return sb.String(), nil
}
