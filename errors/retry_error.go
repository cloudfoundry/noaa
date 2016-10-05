package errors

// RetryError is a type that noaa uses when it encountered an error,
// but is going to retry the operation.  When errors of this type
// are encountered, they should not result in a closed connection.
type RetryError string

// NewRetryError constructs a RetryError from any error.
func NewRetryError(err error) RetryError {
	return RetryError(err.Error())
}

// Error implements error.
func (e RetryError) Error() string {
	return string(e)
}
