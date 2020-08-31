package storage_test

import (
	"bytes"
	"errors"
	"io/ioutil"
	"testing"

	"github.com/nyaruka/mailroom/utils/storage"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/stretchr/testify/assert"
)

type testS3Client struct {
	returnError           error
	headBucketReturnValue *s3.HeadBucketOutput
	getObjectReturnValue  *s3.GetObjectOutput
	putObjectReturnValue  *s3.PutObjectOutput
}

func (c *testS3Client) HeadBucket(*s3.HeadBucketInput) (*s3.HeadBucketOutput, error) {
	if c.returnError != nil {
		return nil, c.returnError
	}
	return c.headBucketReturnValue, nil
}
func (c *testS3Client) GetObject(*s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	if c.returnError != nil {
		return nil, c.returnError
	}
	return c.getObjectReturnValue, nil
}
func (c *testS3Client) PutObject(*s3.PutObjectInput) (*s3.PutObjectOutput, error) {
	if c.returnError != nil {
		return nil, c.returnError
	}
	return c.putObjectReturnValue, nil
}

func TestS3Test(t *testing.T) {
	client := &testS3Client{}
	s3 := storage.NewS3(client, "mybucket")

	assert.NoError(t, s3.Test())

	client.returnError = errors.New("boom")

	assert.EqualError(t, s3.Test(), "boom")
}

func TestS3Get(t *testing.T) {
	client := &testS3Client{}
	s := storage.NewS3(client, "mybucket")

	client.getObjectReturnValue = &s3.GetObjectOutput{
		Body: ioutil.NopCloser(bytes.NewReader([]byte(`HELLOWORLD`))),
	}

	data, err := s.Get("/foo/things")
	assert.NoError(t, err)
	assert.Equal(t, []byte(`HELLOWORLD`), data)

	client.returnError = errors.New("boom")

	_, err = s.Get("/foo/things")
	assert.EqualError(t, err, "error getting S3 object: boom")
}

func TestS3Put(t *testing.T) {
	client := &testS3Client{}
	s := storage.NewS3(client, "mybucket")

	url, err := s.Put("/foo/things", "text/plain", []byte(`HELLOWORLD`))
	assert.NoError(t, err)
	assert.Equal(t, "https://mybucket.s3.amazonaws.com/foo/things", url)

	client.returnError = errors.New("boom")

	_, err = s.Put("/foo/things", "text/plain", []byte(`HELLOWORLD`))
	assert.EqualError(t, err, "error putting S3 object: boom")
}
