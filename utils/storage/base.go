package storage

// Storage is the interface that provides storage of atttachments etc
type Storage interface {
	Name() string
	Test() error
	Get(path string) ([]byte, error)
	Put(path string, contentType string, contents []byte) (string, error)
}
