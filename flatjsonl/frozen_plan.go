package flatjsonl

import (
	"encoding/json"
	"fmt"
	"os"
)

// FrozenPlanVersion is the current version of frozen Parquet column plan format.
const FrozenPlanVersion = 1

// Frozen plan column and meta field names. A partition reserves them so that
// rows containing fields not covered by the plan can still point back to the
// source line instead of dropping the data silently.
const (
	// FrozenUnknownFieldsColumn stores a JSON object of fields that were not
	// part of the frozen plan, keyed by their original flat paths.
	FrozenUnknownFieldsColumn = "._unknown_fields"

	// FrozenUnknownSourceColumn stores a locator (input file and line
	// sequence) of the source row for which unknown fields were captured.
	FrozenUnknownSourceColumn = "._unknown_source"
)

// FrozenPlanColumn describes a single column of the shared Parquet schema.
type FrozenPlanColumn struct {
	// Name is the flat (original) key, e.g. ".host.name".
	Name string `json:"name"`

	// Replaced is the column name after key replacement rules.
	Replaced string `json:"replaced"`

	// Type is the frozen scalar type, one of Type string/int/float/bool/json.
	// A column that saw integers in the planning inputs but an exactly
	// representable decimal later is frozen as float so every hourly file
	// uses the same physical type.
	Type Type `json:"type"`
}

// FrozenPlan is the column plan shared by all hourly outputs of a partition.
// Every hourly Parquet file is written with the same column order and
// physical types, including hours that contain no rows.
type FrozenPlan struct {
	Version int                `json:"version"`
	Columns []FrozenPlanColumn `json:"columns"`

	// UnknownFields is true when the plan includes the reserved columns for
	// fields not covered by the plan and their source row locators.
	UnknownFields bool `json:"unknownFields"`

	// AddSequence is true when the plan was created with sequence numbers,
	// the frozen-write run must keep the same setting.
	AddSequence bool `json:"addSequence"`
}

// SaveFrozenPlan writes the plan as JSON to a file.
func SaveFrozenPlan(fn string, plan FrozenPlan) error {
	b, err := json.MarshalIndent(plan, "", " ")
	if err != nil {
		return fmt.Errorf("marshal frozen plan: %w", err)
	}

	b = append(b, '\n')

	if err := os.WriteFile(fn, b, 0o600); err != nil {
		return fmt.Errorf("write frozen plan %s: %w", fn, err)
	}

	return nil
}

// LoadFrozenPlan reads a plan from a JSON file.
func LoadFrozenPlan(fn string) (FrozenPlan, error) {
	var plan FrozenPlan

	b, err := os.ReadFile(fn)
	if err != nil {
		return plan, fmt.Errorf("read frozen plan %s: %w", fn, err)
	}

	if err := json.Unmarshal(b, &plan); err != nil {
		return plan, fmt.Errorf("decode frozen plan %s: %w", fn, err)
	}

	if plan.Version != FrozenPlanVersion {
		return plan, fmt.Errorf("unsupported frozen plan version %d, expected %d", plan.Version, FrozenPlanVersion)
	}

	if len(plan.Columns) == 0 {
		return plan, fmt.Errorf("frozen plan %s has no columns", fn)
	}

	seen := make(map[string]bool, len(plan.Columns))
	seenName := make(map[string]bool, len(plan.Columns))

	for i, col := range plan.Columns {
		if col.Name == "" {
			return plan, fmt.Errorf("frozen plan column #%d has empty name", i)
		}

		if seenName[col.Name] {
			return plan, fmt.Errorf("frozen plan has duplicated column name %q", col.Name)
		}

		seenName[col.Name] = true

		if col.Replaced == "" {
			col.Replaced = col.Name
			plan.Columns[i] = col
		}

		if seen[col.Replaced] {
			return plan, fmt.Errorf("frozen plan has duplicated replaced column name %q", col.Replaced)
		}

		seen[col.Replaced] = true

		if !isFrozenPlanType(col.Type) {
			return plan, fmt.Errorf("frozen plan column %q has unsupported type %q", col.Name, col.Type)
		}
	}

	return plan, nil
}

func isFrozenPlanType(t Type) bool {
	switch t { //nolint:exhaustive
	case TypeString, TypeInt, TypeFloat, TypeBool, TypeJSON:
		return true
	default:
		return false
	}
}
