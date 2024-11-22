package util

import (
	"errors"
)

const (
	BucketClaimFinalizer = "cosi.objectstorage.k8s.io/bucketclaim-protection"
)

var (
	// Error codes that the central controller will return
	ErrBucketAlreadyExists = errors.New("a bucket already exists that matches the bucket claim")
	ErrInvalidBucketClass  = errors.New("cannot find bucket class with the name specified in the bucket claim")
	ErrNotImplemented      = errors.New("operation not implemented")
)
