# operator-go v0.13 migration

This release moves DorisCluster reconciliation from the legacy `BaseCluster`
stack to the operator-go v0.13 `GenericReconciler` and
`RoleGroupHandler` APIs.

## Compatibility contract

- The existing `spec.frontend`, `spec.backend`, and optional `spec.broker`
  shape remains unchanged.
- Existing StatefulSet, ConfigMap, Service, and PVC names are retained for
  role-group resource names up to 54 characters. FE and BE PVC templates keep
  their existing names and mount paths.
- The legacy status fields remain readable. The framework additionally writes
  `status.observedGeneration` and `status.roleGroups`; the latter is the
  persistent resource ledger used for orphan cleanup.
- An explicit empty `storageClass` continues to mean unset/inherit, matching
  the existing Doris v1alpha1 contract.
- `gracefulShutdownTimeout` retains the Gen 2 boundary behavior: an explicit
  empty string is treated as unset, and valid durations are truncated to whole
  seconds (so `0s` and `500ms` produce an immediate termination value of zero).
- Legacy resource merge behavior is retained: an explicit `resources: {}`
  clears the CPU and memory defaults, and an explicit empty `cpu: {}` clears
  only the CPU defaults. A role-group CPU block replaces the role-level CPU
  block instead of inheriting omitted leaves.
- `cliOverrides` keeps the Gen 2 contract: role-group entries append to role
  entries, the resulting list replaces the container command, and
  `podOverrides` remains the final layer. Role and role-group `podOverrides`
  arrays also retain their legacy append behavior.
- Existing `podOverrides` may continue replacing the config, log, or data mount
  with a differently named Secret, CSI, or PVC volume. The legacy
  `doris-config` volume name remains valid.
- FE and BE configuration keeps the Gen 2 evaluation order, including defining
  `CUR_DATE` and `LOG_DIR` before `JAVA_OPTS`. Whole-file overrides remain
  supported for arbitrary filenames through
  `configOverrides[filename][filename]`. Other per-key entries remain ignored,
  as they were by the previous controller, so dormant values cannot become
  active unexpectedly during the upgrade.
- The role-wide `<cluster>-<role>-internal` and
  `<cluster>-<role>-service` Services are now reconciled once per role. Their
  selectors include every role group instead of depending on map iteration
  order.
- v0.13 adds a headless governing Service at each existing StatefulSet name.
  A pre-existing Service at that slot is accepted only when it is already
  controlled by the same DorisCluster; external objects are never adopted.
- Doris pods continue to use the namespace `default` ServiceAccount unless a
  `podOverrides.spec.serviceAccountName` is explicitly set. This preserves
  existing image-pull secrets, workload identity, and token automount behavior;
  the reconciler-created per-cluster ServiceAccount is intentionally not bound
  to pods during this compatibility release.
- `enableServiceLinks` remains `true` unless explicitly set in `podOverrides`,
  preserving the service environment variables available to existing main,
  init, and sidecar containers.
- DorisCluster `metadata.labels` are not copied to generated resources in this
  compatibility release, matching the legacy controller and avoiding an
  upgrade-time PodTemplate rollout or accidental label-based policy opt-in.

operator-go v0.13 always reconciles a per-cluster ServiceAccount named
`doriscluster-<cluster-name>`. Doris pods deliberately continue to use the
namespace default ServiceAccount during this compatibility release, so the new
account is unused. Before upgrading, verify that this derived name is free. A
ServiceAccount controlled by another object blocks reconciliation; an unowned
manual ServiceAccount at that name is adopted and will be garbage-collected
when the DorisCluster is deleted.

The new role-group governing Service uses the existing StatefulSet name
`<cluster>-<role>-<group>`. Some installations may have manually created that
missing headless Service for stable pod DNS. Before upgrading, either remove
such a Service or place it under the matching DorisCluster controller owner
reference. The migrated controller rejects every unowned or foreign-owned
Service at this slot before changing any role-wide Services.

Role-group names `internal` and `service` are rejected because the framework's
fixed Service name would collide with one of the legacy role-wide Services.
Within FE or BE, role groups whose derived Service slots collide are also
rejected. The usual case is a short-name pair such as `x` and `x-metrics`: the
first group's legacy metrics Service has the same name as the second group's
fixed governing Service. Rename one member of an affected pair before
upgrading. Broker groups are unaffected because Broker has no metrics Service.

operator-go v0.13 hashes newly created role-group resource names whose natural
name is longer than 54 characters. Adopting an existing pre-v0.13 natural name
under that rule would create a parallel StatefulSet and PVC identity. The
migrated controller therefore rejects such an existing group and leaves its
resources untouched. Check for affected groups before the rollout:

```bash
kubectl get dorisclusters.doris.kubedoop.dev --all-namespaces -o json |
  jq -r '
    .items[] as $cluster |
    {frontend: "fe", backend: "be", broker: "broker"} as $roles |
    $roles | to_entries[] as $role |
    (($cluster.spec[$role.key].roleGroups // {}) | keys[]) as $group |
    [$cluster.metadata.namespace, $cluster.metadata.name, $role.value, $group,
     ($cluster.metadata.name + "-" + $role.value + "-" + $group)] | @tsv' |
  awk -F '\t' 'length($5) > 54 { print }'
```

Treat this preflight as mandatory even for disaster-recovery remnants. The
runtime guard can identify an old natural name from its StatefulSet, ConfigMap,
or Service, but cannot safely associate a lone retained PVC after all of those
objects have disappeared. Bypassing the preflight in that PVC-only case can
create a hashed workload with a new claim instead of reattaching the old PVC.

The previous CRD persisted many defaults into existing objects. Those stored
values remain authoritative after the migration, while newly created objects
use the v0.13 handler defaults. In particular, an existing BE storage block
defaulted by the old CRD remains `10Gi`; a new BE role group that omits storage
uses `20Gi`. Set storage and resource values explicitly when identical defaults
across old and new clusters matter.

The Doris v1alpha1 CRD deliberately keeps its previous permissive duration
schema during this compatibility release. Malformed values still fail
reconciliation, while syntactically valid Go durations keep the Gen 2
whole-second truncation behavior.

## Required upgrade order

Helm installs files from a chart's `crds/` directory but does not update those
CRDs during `helm upgrade`. Apply the CRD from the target release before
upgrading the operator. Starting the v0.13-based controller against the old CRD
can prune the new status ledger and prevents reliable orphan cleanup.

For a local chart checkout:

```bash
kubectl apply --server-side \
  -f deploy/helm/doris-operator/crds/crds.yaml
helm upgrade doris-operator deploy/helm/doris-operator
```

For a published OCI chart, replace `X.Y.Z` with the target version:

```bash
helm show crds oci://quay.io/kubedoopcharts/doris-operator \
  --version X.Y.Z | kubectl apply --server-side -f -
helm upgrade doris-operator \
  oci://quay.io/kubedoopcharts/doris-operator --version X.Y.Z
```

After the rollout, confirm that each cluster can persist both new fields:

```bash
kubectl get dorisclusters.doris.kubedoop.dev --all-namespaces \
  -o custom-columns='NAMESPACE:.metadata.namespace,NAME:.metadata.name,OBSERVED:.status.observedGeneration,ROLE_GROUPS:.status.roleGroups'
```

## Scale-down sequencing

FE and BE replica reductions are gated until Doris confirms that the affected
nodes have been removed from its topology. While that safety gate is waiting,
the generic role-group apply phase is paused for the whole cluster. Make
configuration or image rollouts separately from scale-down changes so an
unrelated rollout is not held behind decommissioning.

`decommission` remains the default BE strategy. Doris rejects it when removing
a backend would leave fewer available BEs than a partition's replication
allocation. Before such a reduction, lower the relevant data replication
allocation through Doris first, or set `backendStrategy: force-drop` only when
the destructive removal is intentional and its data risk is acceptable.

To remove an FE or BE role group:

1. Keep the cluster running and set that role group's replicas to zero.
2. Wait until Doris decommissioning and the StatefulSet scale-down complete.
   The cluster annotation
   `doris.kubedoop.dev/safely-scaled-to-zero` then contains the corresponding
   `fe/<group>` or `be/<group>` proof.
3. Remove the role group from the spec.

A role group cannot be removed while the cluster is stopped because zero
StatefulSet replicas do not prove that Doris topology cleanup was completed.
The proof is invalidated whenever the desired replica count becomes positive,
including while stopped. If an old controller or an operational action already
left a group at zero without this annotation, keep the group in the spec,
resume it temporarily with a positive replica count, then scale it to zero
through the migrated controller before removing it.

Doris has no reliable operation for cancelling an in-progress BE decommission,
and an FE `DROP OBSERVER` cannot be undone atomically with a concurrent spec
change. Before either destructive request, the controller records the affected
pods in a durable DorisCluster annotation. Once a lower target has been
accepted, raising or cancelling that target is rejected until the operation
finishes. Restore the previous lower replica target, wait for the StatefulSet to
reach it, and then scale up in a separate change.

## Vector compatibility

When `enableVectorAgent` is true, the role-group ConfigMap continues to contain
`vector.yaml` for a Vector container supplied through `podOverrides`. The
configured `vectorAggregatorConfigMapName` must resolve to a ConfigMap with a
valid `ADDRESS` value. Its pipeline remains the Gen 2 v0.12.6 template; the
v0.13 Vector metrics source and exporter are not added to a user-managed legacy
sidecar.

The official Apache Doris role images used by this operator have not been
validated to contain the Vector binary expected by operator-go's managed
sidecar provider. This compatibility release therefore does not automatically
inject that sidecar. Keep an existing custom Vector container until a compatible
Doris image and lifecycle test are available.

## Release acceptance

Before release, retain evidence for both paths:

- a fresh-cluster Chainsaw run on the supported Kubernetes matrix;
- an in-place old-controller to new-controller comparison of normalized
  StatefulSets, ConfigMaps, Services, PDBs, and ServiceAccounts;
- FE and BE scale-down, role-group removal, PVC identity, and rollback checks.
