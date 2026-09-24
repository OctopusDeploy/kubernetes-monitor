package run

import "fmt"

// CommandLaunchError is an error type that returns when the command fails to launch.
// it will show the usage of the command prompt
type CommandLaunchError struct {
	Err error
}

func (s CommandLaunchError) Error() string {
	return fmt.Sprintf("startup error: %v", s.Err)
}
