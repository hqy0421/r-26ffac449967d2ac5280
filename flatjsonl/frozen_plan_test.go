package flatjsonl_test

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vearutop/flatjsonl/flatjsonl"
)

const (
	frozenPlanPath = "testdata/frozen_plan.json"
	frozenParquet  = "testdata/frozen_hour.parquet"
)

func cleanupFrozen(t *testing.T) {
	t.Helper()

	for _, fn := range []string{frozenPlanPath, frozenParquet} {
		require.NoError(t, os.RemoveAll(fn))
	}

	t.Cleanup(func() {
		for _, fn := range []string{frozenPlanPath, frozenParquet} {
			require.NoError(t, os.RemoveAll(fn))
		}
	})
}

func frozenFlags(input string) flatjsonl.Flags {
	f := flatjsonl.Flags{}
	f.Concurrency = 1
	f.Parquet = frozenParquet
	f.ParquetPlan = frozenPlanPath
	f.Input = input

	return f
}

// TestFrozenPlan_createAndWrite verifies that a plan created from the first
// hour is reused by later hours with the same column order and physical
// types, including an int-to-float promotion and fully missing hours.
func TestFrozenPlan_createAndWrite(t *testing.T) {
	cleanupFrozen(t)

	// First run creates the plan and writes hour 1.
	f := frozenFlags("testdata/frozen_hour1.jsonl")
	f.UnknownFields = true

	proc, err := flatjsonl.NewProcessor(f, flatjsonl.Config{}, f.Inputs()...)
	require.NoError(t, err)
	require.NoError(t, proc.Process())

	planBytes, err := os.ReadFile(frozenPlanPath)
	require.NoError(t, err)

	var plan flatjsonl.FrozenPlan
	require.NoError(t, json.Unmarshal(planBytes, &plan))
	assert.Equal(t, flatjsonl.FrozenPlanVersion, plan.Version)
	assert.True(t, plan.UnknownFields)

	// Score starts as int; the plan must still record it as int since the
	// planning input only had integers.
	colTypes := map[string]flatjsonl.Type{}
	colOrder := make([]string, 0, len(plan.Columns))

	for _, c := range plan.Columns {
		colTypes[c.Replaced] = c.Type
		colOrder = append(colOrder, c.Replaced)
	}

	assert.Equal(t, flatjsonl.TypeInt, colTypes[".score"])
	assert.Equal(t, flatjsonl.TypeString, colTypes["._unknown_fields"])
	assert.Equal(t, flatjsonl.TypeString, colTypes["._unknown_source"])

	// The reserved columns must be the last two, keeping data-column order
	// stable.
	assert.Equal(t, "._unknown_fields", colOrder[len(colOrder)-2])
	assert.Equal(t, "._unknown_source", colOrder[len(colOrder)-1])

	hour1 := readParquetRows(t, frozenParquet)
	require.Len(t, hour1, 2)
	assert.Equal(t, "10", hour1[0][".score"])
	// Rows without unknown fields leave the reserved columns absent (NULL).
	_, hasUnknown := hour1[0]["._unknown_fields"]
	assert.False(t, hasUnknown)

	// Hour 2 introduces decimals in an int column and brand new fields.
	f2 := frozenFlags("testdata/frozen_hour2.jsonl")
	f2.UnknownFields = true

	proc2, err := flatjsonl.NewProcessor(f2, flatjsonl.Config{}, f2.Inputs()...)
	require.NoError(t, err)
	require.NoError(t, proc2.Process())

	pf2 := openParquetFile(t, frozenParquet)
	schemaCols := pf2.Schema().Columns()
	require.NotEmpty(t, schemaCols)

	// The physical type of score must follow the plan (INT64), and the
	// decimal value is written against it without changing the schema.
	scoreType := parquetColumnPhysicalType(t, pf2, ".score")
	assert.Equal(t, parquet.Int64, scoreType)

	hour2 := readParquetRows(t, frozenParquet)
	require.Len(t, hour2, 2)
	assert.Equal(t, "30", hour2[0][".score"]) // 30.5 truncated against the frozen int column
	assert.Equal(t, "40", hour2[1][".score"])

	// Unknown fields point back to the source row and preserve values.
	var unknown map[string]json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(hour2[0]["._unknown_fields"]), &unknown))
	assert.Contains(t, unknown, ".new_field")
	assert.Contains(t, unknown, ".nested.deep")
	assert.Equal(t, "testdata/frozen_hour2.jsonl:1", hour2[0]["._unknown_source"])
	_, hasUnknown = hour2[1]["._unknown_fields"]
	assert.False(t, hasUnknown)

	// Hour 4 has no rows but must still produce a readable, concatenable
	// file with the exact same schema.
	f4 := frozenFlags("testdata/frozen_hour4.jsonl")
	f4.UnknownFields = true

	proc4, err := flatjsonl.NewProcessor(f4, flatjsonl.Config{}, f4.Inputs()...)
	require.NoError(t, err)
	require.NoError(t, proc4.Process())

	pf4 := openParquetFile(t, frozenParquet)
	assert.Empty(t, pf4.RowGroups())
	assert.Equal(t, len(schemaCols), len(pf4.Schema().Columns()))
	assert.Empty(t, readParquetRows(t, frozenParquet))
}

// TestFrozenPlan_promotedFloat ensures a column planned as float stays
// double for every hour, including hours that only contain integers.
func TestFrozenPlan_promotedFloat(t *testing.T) {
	cleanupFrozen(t)

	// Plan from hour 2 where score already has a decimal.
	f := frozenFlags("testdata/frozen_hour2.jsonl")

	proc, err := flatjsonl.NewProcessor(f, flatjsonl.Config{}, f.Inputs()...)
	require.NoError(t, err)
	require.NoError(t, proc.Process())

	planBytes, err := os.ReadFile(frozenPlanPath)
	require.NoError(t, err)

	var plan flatjsonl.FrozenPlan
	require.NoError(t, json.Unmarshal(planBytes, &plan))

	for _, c := range plan.Columns {
		if c.Replaced == ".score" {
			assert.Equal(t, flatjsonl.TypeFloat, c.Type)
		}
	}

	// Hour 1 only has integer scores but must write DOUBLE per the plan.
	f1 := frozenFlags("testdata/frozen_hour1.jsonl")

	proc1, err := flatjsonl.NewProcessor(f1, flatjsonl.Config{}, f1.Inputs()...)
	require.NoError(t, err)
	require.NoError(t, proc1.Process())

	pf := openParquetFile(t, frozenParquet)
	assert.Equal(t, parquet.Double, parquetColumnPhysicalType(t, pf, ".score"))

	rows := readParquetRows(t, frozenParquet)
	require.Len(t, rows, 2)
	assert.Equal(t, "10", rows[0][".score"])
}

// TestFrozenPlan_missingInput verifies a missing hourly file still produces
// a schema-only Parquet file.
func TestFrozenPlan_missingInput(t *testing.T) {
	cleanupFrozen(t)

	f := frozenFlags("testdata/frozen_hour1.jsonl")

	proc, err := flatjsonl.NewProcessor(f, flatjsonl.Config{}, f.Inputs()...)
	require.NoError(t, err)
	require.NoError(t, proc.Process())

	fMissing := frozenFlags("testdata/does-not-exist.jsonl")

	procM, err := flatjsonl.NewProcessor(fMissing, flatjsonl.Config{}, fMissing.Inputs()...)
	require.NoError(t, err)
	require.NoError(t, procM.Process())

	pf := openParquetFile(t, frozenParquet)
	assert.Empty(t, pf.RowGroups())
	assert.Empty(t, readParquetRows(t, frozenParquet))
}

// TestFrozenPlan_rejectsMismatchedOptions guards against silently changing
// the frozen schema by running with incompatible flags.
func TestFrozenPlan_rejectsMismatchedOptions(t *testing.T) {
	cleanupFrozen(t)

	f := frozenFlags("testdata/frozen_hour1.jsonl")
	f.AddSequence = true

	proc, err := flatjsonl.NewProcessor(f, flatjsonl.Config{}, f.Inputs()...)
	require.NoError(t, err)
	require.NoError(t, proc.Process())

	fBad := frozenFlags("testdata/frozen_hour2.jsonl")
	// AddSequence defaults to false, mismatching the plan.
	procBad, err := flatjsonl.NewProcessor(fBad, flatjsonl.Config{}, fBad.Inputs()...)
	require.NoError(t, err)
	err = procBad.Process()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "add-sequence")

	// Plan without unknown columns cannot be consumed with -unknown-fields.
	fBad2 := frozenFlags("testdata/frozen_hour2.jsonl")
	fBad2.AddSequence = true
	fBad2.UnknownFields = true

	procBad2, err := flatjsonl.NewProcessor(fBad2, flatjsonl.Config{}, fBad2.Inputs()...)
	require.NoError(t, err)
	err = procBad2.Process()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown-fields")
}

// TestFrozenPlan_requiresParquet makes sure the plan flag is not accepted
// without a Parquet output.
func TestFrozenPlan_requiresParquet(t *testing.T) {
	cleanupFrozen(t)

	f := flatjsonl.Flags{}
	f.Concurrency = 1
	f.ParquetPlan = frozenPlanPath
	f.Input = "testdata/frozen_hour1.jsonl"

	proc, err := flatjsonl.NewProcessor(f, flatjsonl.Config{}, f.Inputs()...)
	require.NoError(t, err)

	err = proc.Process()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "-parquet")
}

// TestFrozenPlan_rejectsTranspose guards the unsupported combination.
func TestFrozenPlan_rejectsTranspose(t *testing.T) {
	cleanupFrozen(t)

	cj, err := os.ReadFile("testdata/transpose_cfg.json")
	require.NoError(t, err)

	var cfg flatjsonl.Config
	require.NoError(t, json.Unmarshal(cj, &cfg))

	f := frozenFlags("testdata/transpose.jsonl")

	proc, err := flatjsonl.NewProcessor(f, cfg, f.Inputs()...)
	require.NoError(t, err)

	err = proc.Process()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "transpose")
}

// TestFrozenPlan_constColumns makes sure constant columns, which are not
// walked from input lines, survive a plan round trip.
func TestFrozenPlan_constColumns(t *testing.T) {
	cleanupFrozen(t)

	cfg := flatjsonl.Config{IncludeKeys: []string{".a", "const:bar", ".foo"}}

	f := frozenFlags("testdata/coalesce.log")

	proc, err := flatjsonl.NewProcessor(f, cfg, f.Inputs()...)
	require.NoError(t, err)
	require.NoError(t, proc.Process())

	planBytes, err := os.ReadFile(frozenPlanPath)
	require.NoError(t, err)

	var plan flatjsonl.FrozenPlan
	require.NoError(t, json.Unmarshal(planBytes, &plan))

	foundConst := false

	for _, c := range plan.Columns {
		if c.Name == "const:bar" {
			foundConst = true
			assert.Equal(t, flatjsonl.TypeString, c.Type)
		}
	}

	assert.True(t, foundConst)

	f2 := frozenFlags("testdata/coalesce.log")

	proc2, err := flatjsonl.NewProcessor(f2, cfg, f2.Inputs()...)
	require.NoError(t, err)
	require.NoError(t, proc2.Process())

	rows := readParquetRows(t, frozenParquet)
	require.Len(t, rows, 4)
	assert.Equal(t, "bar", rows[0]["const:bar"])
	assert.Equal(t, "bar", rows[3]["const:bar"])
}

// TestFrozenPlan_malformedPlan ensures a hand-edited broken plan is rejected
// rather than silently producing a wrong schema.
func TestFrozenPlan_malformedPlan(t *testing.T) {
	cleanupFrozen(t)

	require.NoError(t, os.WriteFile(frozenPlanPath, []byte("{not json"), 0o600))

	f := frozenFlags("testdata/frozen_hour1.jsonl")

	proc, err := flatjsonl.NewProcessor(f, flatjsonl.Config{}, f.Inputs()...)
	require.NoError(t, err)

	err = proc.Process()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "decode frozen plan")
}

// TestFrozenPlan_unsupportedPlanType rejects plans containing type values the
// current writer can not encode.
func TestFrozenPlan_unsupportedPlanType(t *testing.T) {
	cleanupFrozen(t)

	plan := flatjsonl.FrozenPlan{
		Version: flatjsonl.FrozenPlanVersion,
		Columns: []flatjsonl.FrozenPlanColumn{{
			Name:     ".weird",
			Replaced: ".weird",
			Type:     flatjsonl.Type("array"),
		}},
	}
	require.NoError(t, flatjsonl.SaveFrozenPlan(frozenPlanPath, plan))

	f := frozenFlags("testdata/frozen_hour1.jsonl")

	proc, err := flatjsonl.NewProcessor(f, flatjsonl.Config{}, f.Inputs()...)
	require.NoError(t, err)

	err = proc.Process()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported type")
}

// TestFrozenPlan_columnOrderStable verifies that plan and every hourly file
// expose the same set of columns, which is what makes the files concatenable.
func TestFrozenPlan_columnOrderStable(t *testing.T) {
	cleanupFrozen(t)

	f := frozenFlags("testdata/frozen_hour1.jsonl")
	f.UnknownFields = true

	proc, err := flatjsonl.NewProcessor(f, flatjsonl.Config{}, f.Inputs()...)
	require.NoError(t, err)
	require.NoError(t, proc.Process())

	planBytes, err := os.ReadFile(frozenPlanPath)
	require.NoError(t, err)

	var plan flatjsonl.FrozenPlan
	require.NoError(t, json.Unmarshal(planBytes, &plan))

	expectedColumns := make([]string, 0, len(plan.Columns))
	for _, c := range plan.Columns {
		expectedColumns = append(expectedColumns, c.Replaced)
	}

	f2 := frozenFlags("testdata/frozen_hour2.jsonl")
	f2.UnknownFields = true

	proc2, err := flatjsonl.NewProcessor(f2, flatjsonl.Config{}, f2.Inputs()...)
	require.NoError(t, err)
	require.NoError(t, proc2.Process())

	pf := openParquetFile(t, frozenParquet)
	actualColumns := make([]string, 0, len(expectedColumns))

	for _, path := range pf.Schema().Columns() {
		actualColumns = append(actualColumns, path[len(path)-1])
	}

	assert.ElementsMatch(t, expectedColumns, actualColumns)
}

func parquetColumnPhysicalType(t *testing.T, pf *parquet.File, name string) parquet.Kind {
	t.Helper()

	for _, path := range pf.Schema().Columns() {
		if path[len(path)-1] == name {
			leaf, ok := pf.Schema().Lookup(path...)
			require.True(t, ok)

			return leaf.Node.Type().Kind()
		}
	}

	t.Fatalf("column %q not found", name)

	return 0
}
