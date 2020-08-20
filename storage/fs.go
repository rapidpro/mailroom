package storage

import (
	"io/ioutil"
	"os"
	"path"
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
	return nil
}

func (s *fsStorage) Put(p string, contentType string, contents []byte) (string, error) {
	fullPath := path.Join(s.directory, p)

	err := os.MkdirAll(path.Dir(fullPath), s.perms)
	if err != nil {
		return "", err
	}

	err = ioutil.WriteFile(fullPath, contents, s.perms)
	if err != nil {
		return "", err
	}

	return fullPath, nil
}
