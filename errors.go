package goutils

// ErrorNoDataAvailable no data available error
type ErrorNoDataAvailable struct{}

// Error implement error interface
func (ErrorNoDataAvailable) Error() string {
	return "no data available"
}

// ErrorTimeout operation timed out error
type ErrorTimeout struct{}

// Error implement error interface
func (ErrorTimeout) Error() string {
	return "timeout"
}
