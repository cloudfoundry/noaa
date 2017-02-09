package errors

// RetryTimeoutError is encountered when retrying an operation times out.
type RetryTimeoutError struct {
}

// NewRetryTimeoutError constructs a RetryTimeoutError from any error.
func NewRetryTimeoutError() RetryTimeoutError {
	return RetryTimeoutError{}
}

// Error implements error.
func (e RetryTimeoutError) Error() string {
	return "Retry has timed out. Please ask your Cloud Foundry Operator for support."
}
