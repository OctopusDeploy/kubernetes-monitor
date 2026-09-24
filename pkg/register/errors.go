package register

type SecretNotFoundError struct {
	Err error
}

func (s SecretNotFoundError) Error() string {
	return s.Err.Error()
}
