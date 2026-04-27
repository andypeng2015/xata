package clusters

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/api/errors"
)

// ClusterNotFoundError is returned when trying to retrieve/update/delete a cluster that does not exist
func ClusterNotFoundError(clusterID string) error {
	return status.Errorf(codes.NotFound, "cluster [%s] not found", clusterID)
}

// ClusterAlreadyExistsError is returned when trying to create a cluster that already exists
func ClusterAlreadyExistsError(clusterID string) error {
	return status.Errorf(codes.AlreadyExists, "cluster [%s] already exists", clusterID)
}

// ClusterInvalidParameter is returned when trying to create a cluster with some invalid configuration
func ClusterInvalidParameter(id, parameterName, parameterValue, parameterError string) error {
	return status.Errorf(codes.InvalidArgument, "cluster [%s] invalid parameter: %s has value %s and error is %s", id, parameterName, parameterValue, parameterError)
}

// SecretNotFoundForIDError is returned when trying to retrieve/update/delete a secret that does not exist
func SecretNotFoundForIDError(id string) error {
	return status.Errorf(codes.NotFound, "secret not found for ID: [%s]", id)
}

// BackupInvalidParameter is returned when trying to create a backup with some invalid configuration
func BackupInvalidParameter(id, parameterName, parameterValue, parameterError string) error {
	return status.Errorf(codes.InvalidArgument, "cluster [%s] invalid parameter: %s has value %s and error is %s", id, parameterName, parameterValue, parameterError)
}

func ClusterNotHealthyError(clusterID string) error {
	return status.Errorf(codes.FailedPrecondition, "cluster [%s] is not healthy", clusterID)
}

// ClusterUpdateForbiddenError is returned when a cluster update is forbidden
func ClusterUpdateForbiddenError(clusterID string) error {
	return status.Errorf(codes.PermissionDenied, "cluster [%s] update is forbidden", clusterID)
}

// k8sErrorToGRPCError maps Kubernetes API errors to gRPC errors
func k8sErrorToGRPCError(err error) error {
	switch {
	case errors.IsNotFound(err):
		return status.Errorf(codes.NotFound, "resource not found: %v", err)
	case errors.IsAlreadyExists(err):
		return status.Errorf(codes.AlreadyExists, "resource already exists: %v", err)
	case errors.IsInvalid(err):
		return status.Errorf(codes.InvalidArgument, "invalid resource: %v", err)
	case errors.IsForbidden(err):
		return status.Errorf(codes.PermissionDenied, "forbidden: %v", err)
	case errors.IsConflict(err):
		return status.Errorf(codes.Aborted, "conflict: %v", err)
	default:
		return status.Errorf(codes.Unknown, "kubernetes error: %v", err)
	}
}
