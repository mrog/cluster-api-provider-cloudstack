/*
Copyright 2022 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controllers

import (
	"context"
	"fmt"
	"sigs.k8s.io/cluster-api/util/annotations"
	"strings"
	"time"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	infrav1 "sigs.k8s.io/cluster-api-provider-cloudstack/api/v1beta2"
	csCtrlrUtils "sigs.k8s.io/cluster-api-provider-cloudstack/controllers/utils"
	clusterv1 "sigs.k8s.io/cluster-api/api/v1beta1"
)

//+kubebuilder:rbac:groups=infrastructure.cluster.x-k8s.io,resources=cloudstackmachinestatecheckers,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=infrastructure.cluster.x-k8s.io,resources=cloudstackmachinestatecheckers/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=infrastructure.cluster.x-k8s.io,resources=cloudstackmachinestatecheckers/finalizers,verbs=update
//+kubebuilder:rbac:groups=cluster.x-k8s.io,resources=machines,verbs=get;list;watch;delete
//+kubebuilder:rbac:groups=cluster.x-k8s.io,resources=clusters,verbs=update

// CloudStackMachineStateCheckerReconciliationRunner is a ReconciliationRunner with extensions specific to CloudStack machine state checker reconciliation.
type CloudStackMachineStateCheckerReconciliationRunner struct {
	*csCtrlrUtils.ReconciliationRunner
	MachineStateCheckers  *infrav1.CloudStackMachineStateCheckerList
	ReconciliationSubject *infrav1.CloudStackMachineStateChecker
	FailureDomain         *infrav1.CloudStackFailureDomain
	CAPIMachine           *clusterv1.Machine
	CSMachine             *infrav1.CloudStackMachine
	ConfigurableSettings  *CloudStackMachineStateCheckerReconciliationRunnerConfig
}

type CloudStackMachineStateCheckerReconciliationRunnerConfig struct {
	EnableClusterJoinTimeLimit bool
	ClusterJoinTimeLimit       time.Duration
	RequeueDelay               time.Duration
	DelayAfterUnpause          time.Duration
}

// CloudStackMachineStateCheckerReconciler reconciles a CloudStackMachineStateChecker object
type CloudStackMachineStateCheckerReconciler struct {
	csCtrlrUtils.ReconcilerBase
}

const capcLockAnnotationPrefix = "CAPC-msc-lock-"

// Initialize a new CloudStackMachineStateChecker reconciliation runner with concrete types and initialized member fields.
func NewCSMachineStateCheckerReconciliationRunner() *CloudStackMachineStateCheckerReconciliationRunner {
	// Set concrete type and init pointers.
	runner := &CloudStackMachineStateCheckerReconciliationRunner{
		ReconciliationSubject: &infrav1.CloudStackMachineStateChecker{},
		ConfigurableSettings:  &CloudStackMachineStateCheckerReconciliationRunnerConfig{},
	}
	runner.CAPIMachine = &clusterv1.Machine{}
	runner.CSMachine = &infrav1.CloudStackMachine{}
	runner.FailureDomain = &infrav1.CloudStackFailureDomain{}
	// Setup the base runner. Initializes pointers and links reconciliation methods.
	runner.ReconciliationRunner = csCtrlrUtils.NewRunner(runner, runner.ReconciliationSubject, "CloudStackMachineStateChecker")
	// The machine state checker needs to run when the cluster is paused, so it can unpause the cluster.
	runner.IgnorePause = true
	// TODO: Add a mechanism for making the configurable settings configurable.
	runner.ConfigurableSettings.EnableClusterJoinTimeLimit = true
	runner.ConfigurableSettings.RequeueDelay = 5 * time.Second
	// Give CAPI lots of time to catch up when a cluster is unpaused
	runner.ConfigurableSettings.DelayAfterUnpause = 60 * time.Second
	runner.ConfigurableSettings.ClusterJoinTimeLimit = 5 * time.Minute
	return runner
}

func (r *CloudStackMachineStateCheckerReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	return NewCSMachineStateCheckerReconciliationRunner().
		UsingBaseReconciler(r.ReconcilerBase).
		ForRequest(req).
		WithRequestCtx(ctx).
		RunBaseReconciliationStages()
}

func (r *CloudStackMachineStateCheckerReconciliationRunner) Reconcile() (ctrl.Result, error) {
	return r.RunReconciliationStages(
		r.GetParent(r.ReconciliationSubject, r.CSMachine),
		r.GetParent(r.CSMachine, r.CAPIMachine),
		r.CheckPresent(map[string]client.Object{"CloudStackMachine": r.CSMachine, "Machine": r.CAPIMachine}),
		r.GetFailureDomainByName(func() string { return r.CSMachine.Spec.FailureDomainName }, r.FailureDomain),
		r.AsFailureDomainUser(&r.FailureDomain.Spec),
		func() (ctrl.Result, error) {
			if err := r.removeOrphanedClusterLocks(); err != nil {
				return r.ReturnWrappedError(err, "removing orphaned cluster locks")
			}

			if err := r.CSClient.ResolveVMInstanceDetails(r.CSMachine); err != nil {
				if !strings.Contains(strings.ToLower(err.Error()), "no match found") {
					return r.ReturnWrappedError(err, "failed to resolve VM instance details")
				}
			}

			if r.CSMachine.Status.InstanceState == "" {
				return ctrl.Result{RequeueAfter: r.ConfigurableSettings.RequeueDelay}, nil
			}

			csRunning := r.CSMachine.Status.InstanceState == "Running"
			csTimeInState := r.CSMachine.Status.TimeSinceLastStateChange()
			capiRunning := r.CAPIMachine.Status.Phase == string(clusterv1.MachinePhaseRunning)
			capiProvisioned := r.CAPIMachine.Status.Phase == string(clusterv1.MachinePhaseProvisioned)
			isMgmt := strings.Contains(r.CAPIMachine.Name, "-mgmt")       // TODO: Remove
			capiRunning = capiRunning && isMgmt                           // TODO: Remove
			capiProvisioned = capiProvisioned || (capiRunning && !isMgmt) // TODO: Remove
			var delay time.Duration                                       // TODO: Remove this and use the same time limit for all machines
			if isMgmt {
				delay = r.ConfigurableSettings.ClusterJoinTimeLimit
			} else {
				delay = 0
			}

			// capiTimeout indicates that a new VM is running, but it isn't reachable.
			// The cluster may not recover if the machine isn't replaced.
			capiTimeout := csRunning && capiProvisioned && csTimeInState > delay

			if csRunning && capiRunning {
				r.ReconciliationSubject.Status.Ready = true
			} else if capiTimeout {
				r.Log.Info("CloudStack VM cluster join timed out",
					"csName", r.CSMachine.Name,
					"capiName", r.CAPIMachine.Name,
					"instanceID", r.CSMachine.Spec.InstanceID,
					"csState", r.CSMachine.Status.InstanceState,
					"csTimeInState", csTimeInState.String(),
					"capiPhase", r.CAPIMachine.Status.Phase)

				return r.deleteMachineAndRequeue()
			}

			return ctrl.Result{RequeueAfter: r.ConfigurableSettings.RequeueDelay}, nil
		})
}

func (r *CloudStackMachineStateCheckerReconciliationRunner) ReconcileDelete() (ctrl.Result, error) {
	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *CloudStackMachineStateCheckerReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&infrav1.CloudStackMachineStateChecker{}).
		Complete(r)
}

func (r *CloudStackMachineStateCheckerReconciliationRunner) deleteMachineAndRequeue() (ctrl.Result, error) {
	if r.clusterHasLockForCurrentMachine() {
		if err := r.K8sClient.Delete(r.RequestCtx, r.CAPIMachine); err != nil {
			return r.ReturnWrappedError(err, "deleting CAPI machine")
		}
		r.Log.Info("Deleted CAPI machine", "capiMachine", r.CAPIMachine.Name, "csMachine", r.CSMachine.Name)
		return r.unpauseClusterForMachineDeletion()
	} else {
		r.pauseClusterForMachineDeletion()
		// Give CAPI some time before actually deleting the machine
		return ctrl.Result{RequeueAfter: r.ConfigurableSettings.RequeueDelay}, nil
	}
}

func (r *CloudStackMachineStateCheckerReconciliationRunner) getLockAnnotationKey() string {
	return capcLockAnnotationPrefix + r.CAPIMachine.Name
}

func (r *CloudStackMachineStateCheckerReconciliationRunner) pauseClusterForMachineDeletion() {
	r.CAPICluster.Spec.Paused = true
	annotationKey := r.getLockAnnotationKey()
	annotationValue := r.CAPIMachine.Name
	annotations.AddAnnotations(r.CAPICluster, map[string]string{annotationKey: annotationValue})
	err := r.K8sClient.Update(r.RequestCtx, r.CAPICluster)
	if err != nil {
		r.Log.Error(err, "pausing cluster")
	}
	r.Log.Info("paused cluster so CAPI machine can be safely deleted",
		"cluster", r.CAPICluster.Name, "capiMachine", r.CAPIMachine.Name, "csMachine", r.CSMachine.Name)
}

func (r *CloudStackMachineStateCheckerReconciliationRunner) unpauseClusterForMachineDeletion() (ctrl.Result, error) {
	if r.clusterHasLockForCurrentMachine() {
		// Remove the lock
		delete(r.CAPICluster.Annotations, r.getLockAnnotationKey())
		if len(r.getClusterLocks()) == 0 {
			r.CAPICluster.Spec.Paused = false
		}
		// Using Update() instead of Patch() because the operation needs to fail if another lock was added to the cluster
		// while this process was running.
		err := r.K8sClient.Update(r.RequestCtx, r.CAPICluster)
		if err != nil {
			r.Log.Error(err, "unpausing cluster", "cluster", r.CAPICluster.Name, "capiMachine",
				r.CAPIMachine.Name, "csMachine", r.CSMachine.Name)
			return ctrl.Result{RequeueAfter: r.ConfigurableSettings.RequeueDelay}, nil
		}
		r.Log.Info("unpaused cluster", "cluster", r.CAPICluster.Name, "capiMachine", r.CAPIMachine.Name, "csMachine", r.CSMachine.Name)
		return ctrl.Result{RequeueAfter: r.ConfigurableSettings.DelayAfterUnpause}, nil
	} else {
		r.Log.Info("There's no cluster lock for this machine",
			"cluster", r.CAPICluster.Name, "capiMachine", r.CAPIMachine.Name, "csMachine", r.CSMachine.Name)
		return ctrl.Result{RequeueAfter: r.ConfigurableSettings.RequeueDelay}, nil
	}
}

func (r *CloudStackMachineStateCheckerReconciliationRunner) clusterHasLockForCurrentMachine() bool {
	if !r.CAPICluster.Spec.Paused {
		return false
	}
	locks := r.getClusterLocks()
	_, lockExists := locks[r.getLockAnnotationKey()]
	return lockExists
}

func (r *CloudStackMachineStateCheckerReconciliationRunner) getClusterLocks() map[string]string {
	locks := map[string]string{}
	for key, value := range r.CAPICluster.Annotations {
		if strings.HasPrefix(key, capcLockAnnotationPrefix) {
			locks[key] = value
		}
	}
	return locks
}

// removeOrphanedClusterLocks removes locks that belong to other machines and can safely be removed.  This prevents
// problems with orphaned locks.  Returns an error if locks could not be removed, or if reconciliation needs to restart
// because locks were removed.
func (r *CloudStackMachineStateCheckerReconciliationRunner) removeOrphanedClusterLocks() error {
	clusterModified := false
	lockCount := 0
	currentMachineKey := r.getLockAnnotationKey()
	for key, value := range r.getClusterLocks() {
		lockCount++
		if key == currentMachineKey {
			// Don't remove this lock yet because that would prevent the machine from being deleted in a later step.
			continue
		}
		machine := &clusterv1.Machine{}
		err := r.K8sClient.Get(
			r.RequestCtx,
			client.ObjectKey{
				Namespace: "eksa-system",
				Name:      value,
			},
			machine)
		if err != nil && !strings.Contains(strings.ToLower(err.Error()), "not found") {
			// Can't tell if the machine exists or not.
			r.Log.Error(err, "getting machine to check if cluster lock is still needed")
			continue
		}
		if machine.Status.Phase == string(clusterv1.MachinePhaseProvisioned) ||
			machine.Status.Phase == string(clusterv1.MachinePhaseRunning) {
			// Don't remove this lock because another machine state checker still needs it.
			continue
		}
		r.Log.Info("Found orphaned cluster lock", "key", key)
		delete(r.CAPICluster.Annotations, key)
		clusterModified = true
		lockCount--
	}
	if clusterModified {
		if lockCount == 0 {
			r.CAPICluster.Spec.Paused = false
		}
		// Using Update() instead of Patch() because the operation needs to fail if another lock was added to the cluster
		// while this process was running.
		err := r.K8sClient.Update(r.RequestCtx, r.CAPICluster)
		if err != nil {
			return err
		}
		return fmt.Errorf("orphaned lock(s) found and removed from cluster %s", r.CAPICluster.Name)
	}
	return nil
}
