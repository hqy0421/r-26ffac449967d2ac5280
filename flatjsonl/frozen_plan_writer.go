package flatjsonl

import (
	"encoding/json"
	"strconv"
)

// isFrozenReservedPath reports whether a flat path belongs to the reserved
// frozen-plan columns or reader-injected meta keys, which must never be
// captured as unknown data.
func isFrozenReservedPath(flatPath string) bool {
	switch flatPath {
	case FrozenUnknownFieldsColumn, FrozenUnknownSourceColumn, "._sequence":
		return true
	default:
		return false
	}
}

// captureUnknownValue records a field not covered by the frozen plan. Null
// values carry no information and are skipped; the original JSON encoding is
// preserved so that large integers and decimals are not rounded.
func (wi *writeIterator) captureUnknownValue(l *lineBuf, flatPath string, v Value, raw json.RawMessage) {
	if v.Type == TypeNull {
		return
	}

	if l.unknown == nil {
		l.unknown = map[string]unknownFieldEntry{}
	}

	if _, exists := l.unknown[flatPath]; exists {
		// Repeated paths (e.g. array elements flattened elsewhere, or
		// multiple values merged into one key) keep the first occurrence.
		return
	}

	l.unknown[flatPath] = unknownFieldEntry{
		Path:  flatPath,
		Value: raw,
	}
}

// flushUnknownFields renders captured unknown fields into the reserved row
// values. Rows without unknown fields leave both columns absent (NULL).
func (wi *writeIterator) flushUnknownFields(seq int64, l *lineBuf) error {
	if !wi.captureUnknown || len(l.unknown) == 0 {
		return nil
	}

	encoded, err := sortedUnknownFields(l.unknown)
	if err != nil {
		return err
	}

	l.values[wi.unknownKeyIdx] = Value{
		Type:   TypeString,
		String: encoded,
	}
	l.values[wi.sourceKeyIdx] = Value{
		Type:   TypeString,
		String: wi.inputName + ":" + strconv.FormatInt(seq, 10),
	}

	return nil
}
