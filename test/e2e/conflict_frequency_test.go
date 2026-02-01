package e2e

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

//
// -------- Helpers --------
//

// Kubernetes config
func kubeConfig(t *testing.T) *rest.Config {
	if cfg, err := rest.InClusterConfig(); err == nil {
		return cfg
	}

	kubeconfig := filepath.Join(os.Getenv("HOME"), ".kube", "config")
	cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		t.Fatalf("failed to load kubeconfig: %v", err)
	}
	return cfg
}

// Kubernetes client
func mustKubeClient(t *testing.T) *kubernetes.Clientset {
	client, err := kubernetes.NewForConfig(kubeConfig(t))
	if err != nil {
		t.Fatalf("failed to create kube client: %v", err)
	}
	return client
}

// Create YAML via stdin (supports generateName)
func kubectlCreate(t *testing.T, yaml string) {
	cmd := exec.Command("kubectl", "create", "-f", "-")
	cmd.Stdin = bytes.NewBufferString(yaml)

	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("kubectl create failed: %v\n%s", err, string(out))
	}
}

// Wait until controller pod is running
func waitForController(t *testing.T, client *kubernetes.Clientset, namespace string) {
	timeout := time.After(5 * time.Minute)
	tick := time.Tick(5 * time.Second)

	for {
		select {
		case <-timeout:
			t.Fatalf("timeout waiting for controller pod")
		case <-tick:
			pods, _ := client.CoreV1().Pods(namespace).List(context.TODO(), metav1.ListOptions{})
			for _, p := range pods.Items {
				if p.Status.Phase == corev1.PodRunning {
					return
				}
			}
		}
	}
}

// Count optimistic-lock conflicts
func countConflicts(t *testing.T, namespace string) int {
	cmd := exec.Command(
		"kubectl", "logs",
		"-n", namespace,
		"-l", "app=container-object-storage-interface-controller",
		"--tail=-1",
	)

	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("failed to fetch logs: %v\n%s", err, string(out))
	}

	return strings.Count(string(out), "object has been modified")
}

//
// -------- Test --------
//

func Test_ReconcileConflictFrequency(t *testing.T) {
	const (
		namespace        = "container-object-storage-system"
		failureThreshold = 8
	)

	client := mustKubeClient(t)

	// 1. Ensure controller is running
	waitForController(t, client, namespace)

	// Minimal valid BucketClaim
	bucketClaimYAML := `
apiVersion: objectstorage.k8s.io/v1alpha2
kind: BucketClaim
metadata:
  generateName: test-bc-
spec:
  bucketClassName: default
`

	// 2. Create BucketClaims concurrently (forces reconcile contention)
	var wg sync.WaitGroup

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			kubectlCreate(t, bucketClaimYAML)
		}()
	}

	wg.Wait()

	// 3. Allow reconciliation retries to settle
	time.Sleep(30 * time.Second)

	// 4. Scan controller logs for conflicts
	conflicts := countConflicts(t, namespace)

	// 5. Enforce threshold
	if conflicts >= failureThreshold {
		t.Fatalf(
			"too many object-modified conflicts detected: %d (threshold=%d)",
			conflicts,
			failureThreshold,
		)
	}
}