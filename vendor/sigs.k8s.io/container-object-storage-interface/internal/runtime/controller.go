package runtime

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"time"

	v1alpha1 "sigs.k8s.io/container-object-storage-interface/client/apis/objectstorage/v1alpha1"
	bucketclientset "sigs.k8s.io/container-object-storage-interface/client/clientset/versioned"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	kubeclientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	corev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"

	"github.com/spf13/viper"
)

type addFunc func(ctx context.Context, obj interface{}) error
type updateFunc func(ctx context.Context, old, new interface{}) error
type deleteFunc func(ctx context.Context, obj interface{}) error

type addOp struct {
	Object  interface{}
	AddFunc *addFunc
	Indexer cache.Indexer

	Key string
}

func (a addOp) String() string {
	return a.Key
}

type updateOp struct {
	OldObject  interface{}
	NewObject  interface{}
	UpdateFunc *updateFunc
	Indexer    cache.Indexer

	Key string
}

func (u updateOp) String() string {
	return u.Key
}

type deleteOp struct {
	Object     interface{}
	DeleteFunc *deleteFunc
	Indexer    cache.Indexer

	Key string
}

func (d deleteOp) String() string {
	return d.Key
}

type ObjectStorageController struct {
	LeaseDuration time.Duration
	RenewDeadline time.Duration
	RetryPeriod   time.Duration

	eventBroadcaster record.EventBroadcaster
	eventRecorder    record.EventRecorder

	// Controller
	ResyncPeriod time.Duration
	queue        workqueue.RateLimitingInterface
	threadiness  int

	// Listeners
	BucketListener            BucketListener
	BucketClaimListener       BucketClaimListener
	BucketAccessListener      BucketAccessListener
	BucketClassListener       BucketClassListener
	BucketAccessClassListener BucketAccessClassListener

	// leader election
	leaderLock string
	identity   string

	// internal
	initialized  bool
	bucketClient bucketclientset.Interface
	kubeClient   kubeclientset.Interface

	lockerLock sync.Mutex
	locker     map[types.UID]*sync.Mutex
	opMap      *sync.Map
}

func NewDefaultObjectStorageController(identity string, leaderLockName string, threads int) (*ObjectStorageController, error) {
	rateLimit := workqueue.NewItemExponentialFailureRateLimiter(100*time.Millisecond, 30*time.Second)
	return NewObjectStorageController(identity, leaderLockName, threads, rateLimit)
}

func NewObjectStorageController(identity string, leaderLockName string, threads int, limiter workqueue.RateLimiter) (*ObjectStorageController, error) {
	cfg, err := func() (*rest.Config, error) {
		kubeConfig := viper.GetString("kubeconfig")
		if kubeConfig == "" {
			kubeConfig = os.Getenv("KUBECONFIG")
		}
		if kubeConfig != "" {
			return clientcmd.BuildConfigFromFlags("", kubeConfig)
		}
		return rest.InClusterConfig()
	}()
	if err != nil {
		return nil, err
	}

	kubeClient, err := kubeclientset.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	bucketClient, err := bucketclientset.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	return NewObjectStorageControllerWithClientset(identity, leaderLockName, threads, limiter, kubeClient, bucketClient)
}

func NewObjectStorageControllerWithClientset(identity string, leaderLockName string, threads int, limiter workqueue.RateLimiter, kubeClient kubeclientset.Interface, bucketClient bucketclientset.Interface) (*ObjectStorageController, error) {
	id := identity
	var err error
	if id == "" {
		id, err = os.Hostname()
		if err != nil {
			return nil, err
		}
	}

	rb := record.NewBroadcaster()
	rb.StartRecordingToSink(&corev1.EventSinkImpl{Interface: kubeClient.CoreV1().Events("")})

	extendedScheme := scheme.Scheme
	if err := v1alpha1.AddToScheme(extendedScheme); err != nil {
		return nil, err
	}

	return &ObjectStorageController{
		eventBroadcaster: rb,
		eventRecorder:    rb.NewRecorder(extendedScheme, v1.EventSource{Component: id}),

		identity:     id,
		kubeClient:   kubeClient,
		bucketClient: bucketClient,
		initialized:  false,
		leaderLock:   leaderLockName,
		queue:        workqueue.NewRateLimitingQueue(limiter),
		threadiness:  threads,

		ResyncPeriod: 30 * time.Second,
		// leader election
		LeaseDuration: 150 * time.Second,
		RenewDeadline: 120 * time.Second,
		RetryPeriod:   60 * time.Second,

		opMap: &sync.Map{},
	}, nil
}

// Run - runs the controller. Note that ctx must be cancellable i.e. ctx.Done() should not return nil
func (c *ObjectStorageController) Run(ctx context.Context) error {
	if !c.initialized {
		return fmt.Errorf("Uninitialized controller. Atleast 1 listener should be added")
	}

	ns := func() string {
		if ns := os.Getenv("POD_NAMESPACE"); ns != "" {
			return ns
		}

		if data, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace"); err == nil {
			if ns := strings.TrimSpace(string(data)); len(ns) > 0 {
				return ns
			}
		}
		return "default"
	}()

	id, err := os.Hostname()
	if err != nil {
		return fmt.Errorf("error getting the default leader identity: %v", err)
	}

	c.eventBroadcaster.StartRecordingToSink(&corev1.EventSinkImpl{Interface: c.kubeClient.CoreV1().Events(ns)})
	defer c.eventBroadcaster.Shutdown()

	rlConfig := resourcelock.ResourceLockConfig{
		Identity:      sanitize(id),
		EventRecorder: c.eventRecorder,
	}

	leader := sanitize(fmt.Sprintf("%s/%s", c.leaderLock, c.identity))
	l, err := resourcelock.New(resourcelock.LeasesResourceLock, ns, leader, c.kubeClient.CoreV1(), c.kubeClient.CoordinationV1(), rlConfig)
	if err != nil {
		return err
	}

	leaderConfig := leaderelection.LeaderElectionConfig{
		Lock:            l,
		ReleaseOnCancel: true,
		LeaseDuration:   c.LeaseDuration,
		RenewDeadline:   c.RenewDeadline,
		RetryPeriod:     c.RetryPeriod,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				klog.V(2).InfoS("became leader, starting controller")
				c.runController(ctx)
			},
			OnStoppedLeading: func() {
				klog.InfoS("stopped leading")
			},
			OnNewLeader: func(identity string) {
				klog.V(3).InfoS("new leader detected", "name", identity)
			},
		},
	}

	leaderelection.RunOrDie(ctx, leaderConfig)
	return nil // should never reach here
}

func (c *ObjectStorageController) runWorker(ctx context.Context) {
	for c.processNextItem(ctx) {
	}
}

func (c *ObjectStorageController) processNextItem(ctx context.Context) bool {
	// Wait until there is a new item in the working queue
	uuidInterface, quit := c.queue.Get()
	if quit {
		return false
	}

	uuid := uuidInterface.(types.UID)
	var err error

	defer c.queue.Done(uuid)

	op, ok := c.opMap.Load(uuid)
	if !ok {
		return true
	}

	// Ensure that multiple operations on different versions of the same object
	// do not happen in parallel
	c.OpLock(uuid)
	defer c.OpUnlock(uuid)

	switch o := op.(type) {
	case addOp:
		add := *o.AddFunc
		err = add(ctx, o.Object)
		o.Indexer.Add(o.Object)
	case updateOp:
		update := *o.UpdateFunc
		err = update(ctx, o.OldObject, o.NewObject)
		o.Indexer.Update(o.NewObject)
	case deleteOp:
		delete := *o.DeleteFunc
		err = delete(ctx, o.Object)
		o.Indexer.Delete(o.Object)
		c.opMap.Delete(uuid)
	default:
		panic("unknown item in queue")
	}

	// Handle the error if something went wrong
	c.handleErr(err, uuid)
	return true
}

func (c *ObjectStorageController) OpLock(op types.UID) {
	c.GetOpLock(op).Lock()
}

func (c *ObjectStorageController) OpUnlock(op types.UID) {
	c.GetOpLock(op).Unlock()
}

func (c *ObjectStorageController) GetOpLock(op types.UID) *sync.Mutex {
	lockKey := op
	c.lockerLock.Lock()
	defer c.lockerLock.Unlock()

	if c.locker == nil {
		c.locker = map[types.UID]*sync.Mutex{}
	}

	if _, ok := c.locker[lockKey]; !ok {
		c.locker[lockKey] = &sync.Mutex{}
	}
	return c.locker[lockKey]
}

// handleErr checks if an error happened and makes sure we will retry later.
func (c *ObjectStorageController) handleErr(err error, uuid types.UID) {
	if err == nil {
		c.opMap.Delete(uuid)
		return
	}
	c.queue.AddRateLimited(uuid)
}

func (c *ObjectStorageController) runController(ctx context.Context) {
	controllerFor := func(name string, objType runtime.Object, add addFunc, update updateFunc, delete deleteFunc) {
		indexer := cache.NewIndexer(cache.DeletionHandlingMetaNamespaceKeyFunc, cache.Indexers{})
		resyncPeriod := c.ResyncPeriod

		lw := cache.NewListWatchFromClient(c.bucketClient.ObjectstorageV1alpha1().RESTClient(), name, "", fields.Everything())
		cfg := &cache.Config{
			Queue: cache.NewDeltaFIFOWithOptions(cache.DeltaFIFOOptions{
				KnownObjects:          indexer,
				EmitDeltaTypeReplaced: false,
			}),
			ListerWatcher:    lw,
			ObjectType:       objType,
			FullResyncPeriod: resyncPeriod,
			RetryOnError:     true,
			Process: func(obj interface{}, isInInitialList bool) error {
				for _, d := range obj.(cache.Deltas) {
					switch d.Type {
					case cache.Sync, cache.Replaced, cache.Added, cache.Updated:
						if old, exists, err := indexer.Get(d.Object); err == nil && exists {
							key, err := cache.MetaNamespaceKeyFunc(d.Object)
							if err != nil {
								panic(err)
							}

							if reflect.DeepEqual(d.Object, old) {
								return nil
							}

							uuid := d.Object.(metav1.Object).GetUID()

							c.opMap.Store(uuid, updateOp{
								OldObject:  old,
								NewObject:  d.Object,
								UpdateFunc: &update,
								Key:        key,
								Indexer:    indexer,
							})
							c.queue.Add(uuid)
						} else {
							key, err := cache.MetaNamespaceKeyFunc(d.Object)
							if err != nil {
								panic(err)
							}

							uuid := d.Object.(metav1.Object).GetUID()

							if op, ok := c.opMap.LoadOrStore(uuid, addOp{
								Object:  d.Object,
								AddFunc: &add,
								Key:     key,
								Indexer: indexer,
							}); ok { // If an update to the k8s object happens before add has succeeded
								if _, ok := op.(updateOp); ok {
									err := fmt.Errorf("cannot add already added object: %s", key)
									return err
								}
							}
							c.queue.Add(uuid)
						}
					case cache.Deleted:
						key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(d.Object)
						if err != nil {
							panic(err)
						}

						uuid := d.Object.(metav1.Object).GetUID()
						c.opMap.Store(uuid, deleteOp{
							Object:     d.Object,
							DeleteFunc: &delete,
							Key:        key,
							Indexer:    indexer,
						})
						c.queue.Add(uuid)
					}
				}
				return nil
			},
		}
		ctrlr := cache.New(cfg)

		defer utilruntime.HandleCrash()
		defer c.queue.ShutDown()

		go ctrlr.Run(ctx.Done())

		if !cache.WaitForCacheSync(ctx.Done(), ctrlr.HasSynced) {
			utilruntime.HandleError(fmt.Errorf("Timed out waiting for caches to sync"))
			return
		}

		for i := 0; i < c.threadiness; i++ {
			go c.runWorker(ctx)
		}

		<-ctx.Done()
	}

	if c.BucketListener != nil {
		c.BucketListener.InitializeKubeClient(c.kubeClient)
		c.BucketListener.InitializeBucketClient(c.bucketClient)
		c.BucketListener.InitializeEventRecorder(c.eventRecorder)
		addFunc := func(ctx context.Context, obj interface{}) error {
			return c.BucketListener.Add(ctx, obj.(*v1alpha1.Bucket))
		}
		updateFunc := func(ctx context.Context, old interface{}, new interface{}) error {
			return c.BucketListener.Update(ctx, old.(*v1alpha1.Bucket), new.(*v1alpha1.Bucket))
		}
		deleteFunc := func(ctx context.Context, obj interface{}) error {
			return c.BucketListener.Delete(ctx, obj.(*v1alpha1.Bucket))
		}
		go controllerFor("Buckets", &v1alpha1.Bucket{}, addFunc, updateFunc, deleteFunc)
	}
	if c.BucketClaimListener != nil {
		c.BucketClaimListener.InitializeKubeClient(c.kubeClient)
		c.BucketClaimListener.InitializeBucketClient(c.bucketClient)
		c.BucketClaimListener.InitializeEventRecorder(c.eventRecorder)
		addFunc := func(ctx context.Context, obj interface{}) error {
			return c.BucketClaimListener.Add(ctx, obj.(*v1alpha1.BucketClaim))
		}
		updateFunc := func(ctx context.Context, old interface{}, new interface{}) error {
			return c.BucketClaimListener.Update(ctx, old.(*v1alpha1.BucketClaim), new.(*v1alpha1.BucketClaim))
		}
		deleteFunc := func(ctx context.Context, obj interface{}) error {
			return c.BucketClaimListener.Delete(ctx, obj.(*v1alpha1.BucketClaim))
		}
		go controllerFor("BucketClaims", &v1alpha1.BucketClaim{}, addFunc, updateFunc, deleteFunc)
	}
	if c.BucketAccessListener != nil {
		c.BucketAccessListener.InitializeKubeClient(c.kubeClient)
		c.BucketAccessListener.InitializeBucketClient(c.bucketClient)
		c.BucketAccessListener.InitializeEventRecorder(c.eventRecorder)
		addFunc := func(ctx context.Context, obj interface{}) error {
			return c.BucketAccessListener.Add(ctx, obj.(*v1alpha1.BucketAccess))
		}
		updateFunc := func(ctx context.Context, old interface{}, new interface{}) error {
			return c.BucketAccessListener.Update(ctx, old.(*v1alpha1.BucketAccess), new.(*v1alpha1.BucketAccess))
		}
		deleteFunc := func(ctx context.Context, obj interface{}) error {
			return c.BucketAccessListener.Delete(ctx, obj.(*v1alpha1.BucketAccess))
		}
		go controllerFor("BucketAccesses", &v1alpha1.BucketAccess{}, addFunc, updateFunc, deleteFunc)
	}
	if c.BucketClassListener != nil {
		c.BucketClassListener.InitializeKubeClient(c.kubeClient)
		c.BucketClassListener.InitializeBucketClient(c.bucketClient)
		c.BucketClassListener.InitializeEventRecorder(c.eventRecorder)
		addFunc := func(ctx context.Context, obj interface{}) error {
			return c.BucketClassListener.Add(ctx, obj.(*v1alpha1.BucketClass))
		}
		updateFunc := func(ctx context.Context, old interface{}, new interface{}) error {
			return c.BucketClassListener.Update(ctx, old.(*v1alpha1.BucketClass), new.(*v1alpha1.BucketClass))
		}
		deleteFunc := func(ctx context.Context, obj interface{}) error {
			return c.BucketClassListener.Delete(ctx, obj.(*v1alpha1.BucketClass))
		}
		go controllerFor("BucketClasses", &v1alpha1.BucketClass{}, addFunc, updateFunc, deleteFunc)
	}
	if c.BucketAccessClassListener != nil {
		c.BucketAccessClassListener.InitializeKubeClient(c.kubeClient)
		c.BucketAccessClassListener.InitializeBucketClient(c.bucketClient)
		c.BucketAccessClassListener.InitializeEventRecorder(c.eventRecorder)
		addFunc := func(ctx context.Context, obj interface{}) error {
			return c.BucketAccessClassListener.Add(ctx, obj.(*v1alpha1.BucketAccessClass))
		}
		updateFunc := func(ctx context.Context, old interface{}, new interface{}) error {
			return c.BucketAccessClassListener.Update(ctx, old.(*v1alpha1.BucketAccessClass), new.(*v1alpha1.BucketAccessClass))
		}
		deleteFunc := func(ctx context.Context, obj interface{}) error {
			return c.BucketAccessClassListener.Delete(ctx, obj.(*v1alpha1.BucketAccessClass))
		}
		go controllerFor("BucketAccessClasses", &v1alpha1.BucketAccessClass{}, addFunc, updateFunc, deleteFunc)
	}

	<-ctx.Done()
}

func sanitize(n string) string {
	re := regexp.MustCompile("[^a-zA-Z0-9-]")
	name := strings.ToLower(re.ReplaceAllString(n, "-"))
	if name[len(name)-1] == '-' {
		// name must not end with '-'
		name = name + "X"
	}
	return name
}
