# Lucid Buildfarm fork inventory

This inventory describes the Lucid-only history through internal branch
`ssmith-buildfarm-use-fork-hardening`. That internal branch pins Buildfarm commit
`cced39f72001a8b82d27f2eb2a8c388762965a12`. The comparison base for the old fork is
`4110fd9f`; the clean update base is upstream `main` at
`808050fac62ee81083762e5bd6cf5cd73ce29c86` (2026-08-19).

No branch listed here has been pushed. `lucid-cleanup/pure-upstream` is an exact pointer to the
upstream commit, and `lucid-cleanup/reconstructed` composes the retained changes on top of it.

## How the branches are structured

Each feature branch ultimately descends from `lucid-cleanup/pure-upstream`. Small independent
features contain only their own ported commits. A branch that cannot function independently
contains its prerequisites as earlier commits; this is intentional and is called out below.
For an upstream PR, submit the smallest prerequisite first, rebase its dependent branch, and then
submit only the remaining commits.

Branches whose old change is already in upstream point directly at `pure-upstream`. They document
that the feature was accounted for without manufacturing a no-op commit.

Size is the implementation/review size, not simply the branch diff (dependency-bearing branches
have much larger aggregate diffs). "Upstream fit" estimates how naturally the feature can be
proposed to Buildfarm; it is not a claim that maintainers will accept it.

## Feature inventory

| Feature branch | Old commit(s) | Size | Status on current upstream | Upstream fit / notes |
| --- | --- | --- | --- | --- |
| `action-dedup` | `c759c54d` | Small | Already upstream as `e3293772` | Done; pointer branch only. |
| `dynamic-report-result-width` | `c0a299f0` | Small | Ported | High. A focused configuration option with a small test surface. |
| `persistent-worker-as-nobody` | `69b68dcf` | Small | Ported | Medium-high. Explain why persistent workers cannot use the per-action nobody lifecycle. |
| `bzlmod-cleanup` | `4b9b0af3`, `cd852a78` | Small | Ported, but potentially stale | Medium. Revalidate against upstream's current dependency policy before proposing. |
| `output-path-linking` | `389b80ad` | Small | Already upstream as `9a0fa202` | Done; pointer branch only. |
| `persistent-worker-output-paths` | `b20df1e2` | Medium | Ported | High. Residual persistent-worker output movement fix; independent of the upstreamed linking fix. |
| `cas-size-accounting` | `1b267549`, `3a271434`, `4f4fe3f5` | Large | Ported | Medium-high. Three separable fixes: directory overhead, startup race, and filesystem block accounting. Prefer three ordered PRs. |
| `json-logging` | `3e9df139`, `c9412d44` | Medium | Ported | Medium. General-purpose, but introduces logging dependencies/configuration and a lockfile update. |
| `cas-max-size-percent` | `8b214cc6` | Medium | Ported | High. Focused configuration feature with validation/tests. |
| `persistent-worker-lifecycle` | `e3980c5a`, `62ae7ca3`, `66c85a48`, `95df7e07`, `fc3b8508`, `acd42ec7` | Large | Ported | Medium-high. Submit as ordered bug-fix PRs: timeout NPE, pending-request cleanup, exec-root/output cleanup, invalidation, exception propagation, then interrupt propagation. |
| `action-cache-size` | `4e27e169` | Small | Already upstream as `51444cef` | Done; pointer branch only. |
| `stub-write-memory-leak` | `cdf76656` | Large | Ported | High. Focused correctness/resource-lifetime fix despite its test volume. Also included as a prerequisite in `write-upload-lifecycle`. |
| `write-upload-lifecycle` | `f45724a9`, `977fec91`, `8165b2ba`, `92d29365`, `70e8bc79`, `fc91c63b`, `bc6f5979`, `22f1a283`, `16f843bf`, `1b0a23b3`, `51ee0002`, `c337bc0a` | Very large | Ported | Medium. Includes `stub-write-memory-leak` as a prerequisite. Split into the Write cancellation contract, reusable upload helper, observer lifecycle fixes, then consumer migrations. |
| `cas-eviction-diagnostics` | `235da7db` | Medium | Ported | High. Focused performance/diagnostic gating change. |
| `symlink-input-validation` | `f2b631fc` | Large | Ported | High. Security/correctness fix; keep its adversarial tests together. |
| `atomic-file-writer` | `0b805221` | Small | Ported | High. Focused atomicity fix. |
| `cas-concurrency` | `c88af19c`, `c8430b0e`, `e03947c3`, `edf1737f`, `ba3b48ec` | Very large | Ported | Low as one PR, medium as a series. Depends on size accounting, percent sizing, upload lifecycle, eviction diagnostics, symlink validation, and atomic file writing. Split into refcount state, async eviction, sharding/snapshots, backpressure, and metrics. |
| `directory-hardlinks` | `1405e89a`, `1c34501b` | Very large | Ported | Medium as a series. Depends on the complete `cas-concurrency` branch. Submit hardlink materialization/accounting before metrics. |
| `persistent-worker-metrics` | `a4ceaf5e` | Large | Ported | Medium. Branch includes the persistent-worker correctness prerequisites and output-path handling. Upstream only the final metrics delta after those land. |
| `worker-registration-recovery` | `b0a0c6d7`, `28c8b493` | Medium | Ported | High. Ordered outage-recovery fix and health-ordering follow-up. |
| `redis-reconnect-backoff` | `5bb8f742` | Small | Ported | High. Focused reconnect-loop hardening. |
| `provision-queue-metrics` | `f9299116` | Medium | Ported | Medium-high. Useful generic metric, but includes API/protobuf shape decisions. |
| `allowlisted-remote-persistent-workers` | `cced39f7` | Medium | Ported | Medium. Depends on the persistent-worker branch family. Clearly document the allowlist and tool-input trust model. |
| `cgroup-v2-memory` | new local fix (`1b7cc2e6` on its feature branch) | Small | Ported | High. Updates the memory controller to cgroup v2 (`memory.max`, `memory.swap.max`) and uses 64-bit parsing. This is newer than the named cutoff and is included because it enables current Buildfarm in Lucid's containers. |

The two formatting-only old commits, `12f1c92f` and `19e198d4`, are not feature branches.
`19e198d4` is represented upstream by `aad1cd4b`; `12f1c92f` has no behavioral delta worth
carrying. This accounts for all 51 old commits: 46 retained changes were ported, three behavioral
changes are represented by upstream pointer branches, and two are formatting-only.

## Dependency families used by the reconstruction

The reconstruction deliberately consumes two cumulative branch families to avoid replaying the
same prerequisites twice:

1. `directory-hardlinks` brings in CAS size accounting, percent sizing, the native-memory and
   upload lifecycle work, eviction diagnostics, symlink validation, atomic writes, CAS
   concurrency, and hardlink materialization/metrics.
2. `allowlisted-remote-persistent-workers` brings in nobody handling, output-path movement,
   persistent-worker lifecycle fixes, persistent-worker metrics, and remote persistent-worker
   eligibility.

The reconstruction then adds the independent dynamic-width, Bzlmod, JSON logging, worker
registration, Redis backoff, provision-queue metric, and cgroup-v2 features. Upstreamed pointer
features require no reconstruction commit because they are already in the base.

## Verification notes

The CAS concurrency series adds JCTools and changes the Maven dependency graph. The reconstructed
branch's `maven_install.json` was repinned against its final `MODULE.bazel`, and Bazel validates the
resulting lock signature. Maven Central returned HTTP 429 for some transitive POM requests during
verification, so the repin used Google's read-only Maven Central mirror and restored canonical
Central URLs in the checked-in lockfile. Targeted CAS, worker, shard-worker, persistent-worker, and
cgroup test suites pass; the test invocation used a temporary local override of grpc-java whose
only change was the same repository mirror substitution. That override is not part of the branch.

## Suggested upstream order

Start with the small correctness changes (`cgroup-v2-memory`, `redis-reconnect-backoff`,
`atomic-file-writer`, `cas-eviction-diagnostics`, and `dynamic-report-result-width`). Then submit
the CAS sizing and upload-lifecycle series, followed by CAS concurrency and hardlinks. Submit the
persistent-worker correctness series before its metrics and allowlisting changes. Keep Lucid
deployment/configuration changes outside Buildfarm PRs unless they expose a generally useful
container contract.
