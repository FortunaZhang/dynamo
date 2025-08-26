package dynamo

import (
	"context"
	"fmt"
	"strings"

	grovev1alpha1 "github.com/NVIDIA/grove/operator/api/core/v1alpha1"
	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	nvidiacomv1alpha1 "github.com/ai-dynamo/dynamo/deploy/cloud/operator/api/v1alpha1"
	commonconsts "github.com/ai-dynamo/dynamo/deploy/cloud/operator/internal/consts"
)

type GroveMultinodeDeployer struct {
	MultinodeDeployer
}

func (d *GroveMultinodeDeployer) GetLeaderHostname(serviceName string) string {
	return fmt.Sprintf("${GROVE_PCSG_NAME}-${GROVE_PCSG_INDEX}-%s-%s-0.${GROVE_HEADLESS_SERVICE}", serviceName, commonconsts.GroveRoleSuffixLeader)
}

func (d *GroveMultinodeDeployer) GetNodeRank() string {
	return "$((GROVE_PCLQ_POD_INDEX + 1))"
}

func (d *GroveMultinodeDeployer) GetHostNames(serviceName string, numberOfNodes int32) []string {
	hostnames := make([]string, 0, numberOfNodes)
	leaderHostname := d.GetLeaderHostname(serviceName)
	hostnames = append(hostnames, leaderHostname)
	// Add worker hostnames
	for i := int32(0); i < numberOfNodes-1; i++ {
		workerHostname := fmt.Sprintf("${GROVE_PCSG_NAME}-${GROVE_PCSG_INDEX}-%s-%s-%d.${GROVE_HEADLESS_SERVICE}",
			serviceName, commonconsts.GroveRoleSuffixWorker, i)
		hostnames = append(hostnames, workerHostname)
	}
	return hostnames
}

// EvaluateAllComponentsReady determines if all Grove components are ready
// - PodCliques: spec.replicas == status.readyReplicas
// - PodCliqueScalingGroups: spec.replicas == status.availableReplicas
func EvaluateAllComponentsReady(ctx context.Context, client client.Client, dgd *nvidiacomv1alpha1.DynamoGraphDeployment) bool {
	logger := log.FromContext(ctx)

	replicaIndex := 0
	for serviceName, component := range dgd.Spec.Services {
		numberOfNodes := component.GetNumberOfNodes()
		isMultinode := numberOfNodes > 1
		resourceName := fmt.Sprintf("%s-%d-%s", dgd.Name, replicaIndex, strings.ToLower(serviceName))

		if isMultinode {
			// Check PodCliqueScalingGroup: spec.replicas == status.availableReplicas
			if !isPCSGReady(ctx, client, resourceName, dgd.Namespace, logger) {
				return false
			}
		} else {
			// Check PodClique: spec.replicas == status.readyReplicas
			if !isPodCliqueReady(ctx, client, resourceName, dgd.Namespace, logger) {
				return false
			}
		}
	}

	return true
}

// isPodCliqueReady checks if a PodClique has spec.replicas == status.readyReplicas
func isPodCliqueReady(ctx context.Context, client client.Client, resourceName, namespace string, logger logr.Logger) bool {
	podClique := &grovev1alpha1.PodClique{}
	err := client.Get(ctx, types.NamespacedName{Name: resourceName, Namespace: namespace}, podClique)
	if err != nil {
		if errors.IsNotFound(err) {
			logger.V(2).Info("PodClique not found", "resourceName", resourceName)
			return false
		}
		logger.V(1).Info("Failed to get PodClique", "error", err, "resourceName", resourceName)
		return false
	}

	desiredReplicas := podClique.Spec.Replicas
	readyReplicas := podClique.Status.ReadyReplicas

	if desiredReplicas == 0 {
		// No replicas desired, so it's ready
		return true
	}

	isReady := desiredReplicas == readyReplicas
	if !isReady {
		logger.V(1).Info("PodClique not ready", "resourceName", resourceName, "desired", desiredReplicas, "ready", readyReplicas)
	}

	return isReady
}

// isPCSGReady checks if a PodCliqueScalingGroup has spec.replicas == status.availableReplicas
func isPCSGReady(ctx context.Context, client client.Client, resourceName, namespace string, logger logr.Logger) bool {
	pcsg := &grovev1alpha1.PodCliqueScalingGroup{}
	err := client.Get(ctx, types.NamespacedName{Name: resourceName, Namespace: namespace}, pcsg)
	if err != nil {
		if errors.IsNotFound(err) {
			logger.V(2).Info("PodCliqueScalingGroup not found", "resourceName", resourceName)
			return false
		}
		logger.V(1).Info("Failed to get PodCliqueScalingGroup", "error", err, "resourceName", resourceName)
		return false
	}

	desiredReplicas := pcsg.Spec.Replicas
	availableReplicas := pcsg.Status.AvailableReplicas

	if desiredReplicas == 0 {
		// No replicas desired, so it's ready
		return true
	}

	isReady := desiredReplicas == availableReplicas
	if !isReady {
		logger.V(1).Info("PodCliqueScalingGroup not ready", "resourceName", resourceName, "desired", desiredReplicas, "available", availableReplicas)
	}

	return isReady
}
