package errors

import "fmt"

// NonRetryError is a type that noaa uses when it encountered an error,
// and is not going to retry the operation.  When errors of this type
// are encountered, they should result in a closed connection.
type NonRetryError string

// NewNonRetryError constructs a NonRetryError from any error.
func NewNonRetryError(err error) NonRetryError {
	return NonRetryError(err.Error())
}

// Error implements error.
func (e NonRetryError) Error() string {
	return fmt.Sprintf("Please ask your Cloud Foundry Operator to check the platform configuration: %s", string(e))
}
