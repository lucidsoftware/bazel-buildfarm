# Persistent worker resource ownership

Persistent workers own their compiler process, descendants, private execution directory, and
stable cgroup. A request owns an exclusive lifecycle lease and an active CPU allocation.
Request completion does not destroy process-owned resources.

## Cgroup layout and compatibility

Normal actions retain their existing per-operation cgroups. Persistent workers use
`executions/persistent-workers/<process UUID>` below the same execution parent, so the common
execution CPU ceiling still applies. The operation's resource handle does not own this cgroup.

The process-specific cgroup wrapper is attached at process creation, after choosing the pool key.
Operation IDs and cgroup launch paths therefore do not split otherwise compatible compiler keys.
The key also contains the stable resource profile (cgroup parent, wrapper, and memory limit).
Different memory limits use different pools. Tool hashes, environment, and existing execution
wrapper arguments remain part of compiler compatibility as before. Per-operation sandbox or
custom wrapper arguments can still prevent reuse; this change does not strip those restrictions.

The existing path without cgroup enforcement remains supported. Freezing and cgroup accounting
apply to managed workers. Managed workers require cgroup v2 with `cgroup.freeze`, `memory.max`,
and `cgroup.kill` support; startup fails if group-wide termination is unavailable.

## Request execution

1. Acquire a worker and its generation-checked lifecycle lease.
2. Prepare request inputs while the compiler is inactive.
3. Install the current CPU allocation, snapshot cumulative counters, and unfreeze the compiler.
4. Submit the protocol request. Use the same CPU adjustment loop as native execution, waiting
   for the protocol response instead of process exit.
5. Stop monitoring and confirm the compiler is frozen before copying outputs and releasing
   the lifecycle lease. Report CPU counter differences for this request, not process-lifetime totals.
6. Release request CPU capacity. Keep the frozen compiler and its retained memory for reuse.

Resource mutations are guarded by the exact lifecycle generation. A callback from a previous
request cannot change the quota or resume/freeze a worker already serving another request.

Timeout and interruption invalidate the lease, cancel outstanding CPU orders, and retire the
worker. Cancelling the response-reader task interrupts its wait; process termination closes
its pipes if it is blocked in protocol I/O. The coordinator retains its lease-guarded watchdog
covering setup/execution. For market executions this permits twice the original action timeout,
matching the monitor's single extension, while imposing a hard bound including input setup.

## Idle CPU and memory policy

Idle managed workers are frozen, with confirmation bounded to five seconds. They consume no
background execution CPU while idle; background GC pauses until the next request. A newly
created group's launch wrapper also joins a frozen group and cannot start the compiler until
the request's CPU quota is installed.

Memory limits remain stable for the life of a PW and use the cgroup-v2 `memory.max` interface.
An unlimited request has an unlimited profile; a limited request cannot reuse that worker.
Retained memory remains charged to the PW while idle. There is no new global memory budget:
operators still need to size the process count, memory limits, and idle-retirement policy together.

## Termination and shutdown

Retirement sends graceful signals, thaws idle workers to allow exit, and waits for the configured
grace. If necessary it uses `cgroup.kill`, then waits for an empty group with a second bounded
wait. Forced fallback termination also targets the group, including descendants whose parent
has exited. Directory cleanup follows only confirmed termination. Failures are reported and
the execution directory is preserved when termination cannot be confirmed.

Shutdown drains the execution pipeline and closes the PW pool before dismantling the shared
cgroup hierarchy. Existing native process cleanup stays on its per-action path.

## Validation

Local tests cover stable resource profiles, failed launches, reuse across operations,
freeze-before-pool-return ordering, stale resource leases, per-request CPU accounting,
CPU-market quota reduction, cancellation, and cgroup-v2 control-file behavior.

A separate kernel-backed integration target verifies reuse of a real compiler across two
requests, frozen CPU inactivity, quota and memory limits, pool-shutdown cleanup, and retirement
of an orphaned child. It requires an explicitly delegated writable test parent with CPU and
memory subtree controllers already enabled. It creates and removes only uniquely named children;
it does not reconfigure the parent or move the development session into another cgroup.

```sh
bazel test //src/test/java/build/buildfarm/worker/cgroup:PersistentWorkerCgroupIntegrationTest \
  --test_tag_filters=integration \
  --jvmopt=-Dbuildfarm.pw.cgroupParent=/sys/fs/cgroup/DELEGATED_TEST_PARENT \
  --test_output=errors
```

The integration target compiles but has not run in this workspace: no writable delegated
cgroup is available, and the user systemd manager is inaccessible. Run it in the worker-image
validation environment before deployment.
