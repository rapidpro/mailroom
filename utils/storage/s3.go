package storage

import (
	"bytes"
	"fmt"
	"io/ioutil"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/pkg/errors"
)

var s3BucketURL = "https://%s.s3.amazonaws.com%s"

// S3Client provides a mockable subset of the S3 API
type S3Client interface {
	HeadBucket(*s3.HeadBucketInput) (*s3.HeadBucketOutput, error)
	GetObject(*s3.GetObjectInput) (*s3.GetObjectOutput, error)
	PutObject(*s3.PutObjectInput) (*s3.PutObjectOutput, error)
}

// S3Options are options for an S3 client
type S3Options struct {
	AWSAccessKeyID     string
	AWSSecretAccessKey string
	Endpoint           string
	Region             string
	DisableSSL         bool
	ForcePathStyle     bool
}

// NewS3Client creates a new S3 client
func NewS3Client(opts *S3Options) (S3Client, error) {
	s3Session, err := session.NewSession(&aws.Config{
		Credentials:      credentials.NewStaticCredentials(opts.AWSAccessKeyID, opts.AWSSecretAccessKey, ""),
		Endpoint:         aws.String(opts.Endpoint),
		Region:           aws.String(opts.Region),
		DisableSSL:       aws.Bool(opts.DisableSSL),
		S3ForcePathStyle: aws.Bool(opts.ForcePathStyle),
	})
	if err != nil {
		return nil, err
	}

	return s3.New(s3Session), nil
}

type s3Storage struct {
	client S3Client
	bucket string
}

// NewS3 creates a new S3 storage service
func NewS3(client S3Client, bucket string) Storage {
	return &s3Storage{client: client, bucket: bucket}
}

func (s *s3Storage) Name() string {
	return "S3"
}

// Test tests whether our S3 client is properly configured
func (s *s3Storage) Test() error {
	_, err := s.client.HeadBucket(&s3.HeadBucketInput{
		Bucket: aws.String(s.bucket),
	})
	return err
}

func (s *s3Storage) Get(path string) ([]byte, error) {
	out, err := s.client.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(path),
	})
	if err != nil {
		return nil, errors.Wrapf(err, "error getting S3 object")
	}

	body, err := ioutil.ReadAll(out.Body)
	if err != nil {
		return nil, errors.Wrapf(err, "error reading S3 object")
	}

	return body, nil
}

// Put writes the passed in file to the bucket with the passed in content type
func (s *s3Storage) Put(path string, contentType string, contents []byte) (string, error) {
	_, err := s.client.PutObject(&s3.PutObjectInput{
		Bucket:      aws.String(s.bucket),
		Body:        bytes.NewReader(contents),
		Key:         aws.String(path),
		ContentType: aws.String(contentType),
		ACL:         aws.String(s3.BucketCannedACLPublicRead),
	})
	if err != nil {
		return "", errors.Wrapf(err, "error putting S3 object")
	}

	return s.url(path), nil
}

func (s *s3Storage) url(path string) string {
	return fmt.Sprintf(s3BucketURL, s.bucket, path)
}
