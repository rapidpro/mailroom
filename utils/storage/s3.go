package storage

import (
	"bytes"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/nyaruka/mailroom/config"
	"github.com/sirupsen/logrus"
)

var s3BucketURL = "https://%s.s3.amazonaws.com%s"

type s3Storage struct {
	client s3iface.S3API
	bucket string
}

// NewS3 creates a new S3 storage service
func NewS3(cfg *config.Config) (Storage, error) {
	s3Session, err := session.NewSession(&aws.Config{
		Credentials:      credentials.NewStaticCredentials(cfg.AWSAccessKeyID, cfg.AWSSecretAccessKey, ""),
		Endpoint:         aws.String(cfg.S3Endpoint),
		Region:           aws.String(cfg.S3Region),
		DisableSSL:       aws.Bool(cfg.S3DisableSSL),
		S3ForcePathStyle: aws.Bool(cfg.S3ForcePathStyle),
	})
	if err != nil {
		return nil, err
	}

	return &s3Storage{
		client: s3.New(s3Session),
		bucket: cfg.S3MediaBucket,
	}, nil
}

func (s *s3Storage) Name() string {
	return "S3"
}

// Test tests whether our S3 client is properly configured
func (s *s3Storage) Test() error {
	params := &s3.HeadBucketInput{
		Bucket: aws.String(s.bucket),
	}
	_, err := s.client.HeadBucket(params)
	return err
}

// Put writes the passed in file to the bucket with the passed in content type
func (s *s3Storage) Put(path string, contentType string, contents []byte) (string, error) {
	params := &s3.PutObjectInput{
		Bucket:      aws.String(s.bucket),
		Body:        bytes.NewReader(contents),
		Key:         aws.String(path),
		ContentType: aws.String(contentType),
		ACL:         aws.String(s3.BucketCannedACLPublicRead),
	}

	logrus.WithField("path", path).Info("** uploading s3 file")

	_, err := s.client.PutObject(params)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf(s3BucketURL, s.bucket, path), nil
}
