package storage

import (
	"io/ioutil"
	"os"
	"path/filepath"
)

type fsStorage struct {
	directory string
	perms     os.FileMode
}

// NewFS creates a new file system storage service suitable for use in tests
func NewFS(directory string) Storage {
	return &fsStorage{directory: directory, perms: 0766}
}

func (s *fsStorage) Name() string {
	return "file system"
}

func (s *fsStorage) Test() error {
	path, err := s.Put("test.txt", "text/plain", []byte(`test`))
	if err != nil {
		return err
	}

	os.Remove(path)
	return nil
}

func (s *fsStorage) Put(path string, contentType string, contents []byte) (string, error) {
	fullPath := filepath.Join(s.directory, path)

	err := os.MkdirAll(filepath.Dir(fullPath), s.perms)
	if err != nil {
		return "", err
	}

	err = ioutil.WriteFile(fullPath, contents, s.perms)
	if err != nil {
		return "", err
	}

	return fullPath, nil
}
