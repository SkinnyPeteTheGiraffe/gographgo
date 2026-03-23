package checkpoint

import (
	"errors"
	"fmt"
)

// PersistenceCompatibilityPolicy describes the storage interoperability
// guarantee for persisted checkpoints.
//
// Current policy: persisted checkpoint rows are guaranteed to be compatible
// only with gographgo runtimes that implement this checkpoint schema.
const PersistenceCompatibilityPolicy = "go-runtime-only"

// ErrUnsupportedPersistenceFormat indicates that checkpoint rows are in a
// format this runtime does not support under the active policy.
var ErrUnsupportedPersistenceFormat = errors.New("checkpoint: unsupported persistence format")

// UnsupportedPersistenceFormatError carries backend-specific details when
// persisted checkpoint rows cannot be decoded with the active policy.
type UnsupportedPersistenceFormatError struct {
	Backend string
	Detail  string
	Cause   error
}

func (e *UnsupportedPersistenceFormatError) Error() string {
	backend := e.Backend
	if backend == "" {
		backend = "checkpoint"
	}
	if e.Detail == "" {
		if e.Cause == nil {
			return fmt.Sprintf("%s: %v", backend, ErrUnsupportedPersistenceFormat)
		}
		return fmt.Sprintf("%s: %v: %v", backend, ErrUnsupportedPersistenceFormat, e.Cause)
	}
	if e.Cause == nil {
		return fmt.Sprintf("%s: %v: %s", backend, ErrUnsupportedPersistenceFormat, e.Detail)
	}
	return fmt.Sprintf("%s: %v: %s: %v", backend, ErrUnsupportedPersistenceFormat, e.Detail, e.Cause)
}

func (e *UnsupportedPersistenceFormatError) Unwrap() error {
	if e.Cause == nil {
		return ErrUnsupportedPersistenceFormat
	}
	return errors.Join(ErrUnsupportedPersistenceFormat, e.Cause)
}

// NewUnsupportedPersistenceFormatError creates a policy error for unsupported
// persisted checkpoint rows.
func NewUnsupportedPersistenceFormatError(backend, detail string, cause error) error {
	return &UnsupportedPersistenceFormatError{Backend: backend, Detail: detail, Cause: cause}
}

// IsUnsupportedPersistenceFormat reports whether err indicates unsupported
// persisted checkpoint format under the active policy.
func IsUnsupportedPersistenceFormat(err error) bool {
	return errors.Is(err, ErrUnsupportedPersistenceFormat)
}
