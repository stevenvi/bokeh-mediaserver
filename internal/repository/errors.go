package repository

import "errors"

// ErrNotFound is returned when a record is not found or no rows were affected.
var ErrNotFound = errors.New("not found")
