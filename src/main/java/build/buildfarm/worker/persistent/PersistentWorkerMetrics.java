// Copyright 2026 The Buildfarm Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package build.buildfarm.worker.persistent;

import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import persistent.bazel.client.PersistentWorker;
import persistent.bazel.client.WorkerKey;

/** Low-cardinality Prometheus metrics for remote persistent-worker requests and processes. */
final class PersistentWorkerMetrics {
  static final String OUTCOME_SUCCESS = "success";
  static final String OUTCOME_ACTION_FAILURE = "action_failure";
  static final String OUTCOME_POOL_TIMEOUT = "pool_timeout";
  static final String OUTCOME_WORKER_TIMEOUT = "worker_timeout";
  static final String OUTCOME_WORKER_ERROR = "worker_error";
  static final String OUTCOME_TOOL_SETUP_FAILURE = "tool_setup_failure";
  static final String OUTCOME_INPUT_SETUP_FAILURE = "input_setup_failure";
  static final String OUTCOME_OUTPUT_CLEANUP_FAILURE = "output_cleanup_failure";
  static final String OUTCOME_INPUT_CLEANUP_FAILURE = "input_cleanup_failure";
  static final String OUTCOME_INTERRUPTED = "interrupted";
  static final String PHASE_POOL_WAIT = "pool_wait";
  static final String PHASE_WORKER_START = "worker_start";
  static final String PHASE_TOOL_SETUP = "tool_setup";
  static final String PHASE_INPUT_SETUP = "input_setup";
  static final String PHASE_WORKER_EXECUTION = "worker_execution";
  static final String PHASE_OUTPUT_MOVE = "output_move";
  static final String PHASE_INPUT_CLEANUP = "input_cleanup";

  static final String METHOD_COPY = "copy";
  static final String METHOD_TOOL_COPY = "tool_copy";
  static final String DESTROY_POOL = "pool";
  static final String DESTROY_REQUEST_FAILURE = "request_failure";
  static final String DESTROY_TIMEOUT = "timeout";
  static final String DESTROY_UNEXPECTED_EXIT = "unexpected_exit";

  private static final double[] TIME_BUCKETS = {
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60
  };

  private static final Counter requests =
      Counter.build()
          .name("persistent_worker_requests_total")
          .labelNames("outcome")
          .help("Persistent-worker requests by bounded outcome.")
          .register();
  private static final Histogram requestSeconds =
      Histogram.build()
          .name("persistent_worker_request_seconds")
          .labelNames("outcome")
          .buckets(TIME_BUCKETS)
          .help("End-to-end wall time of a persistent-worker request.")
          .register();
  private static final Histogram phaseSeconds =
      Histogram.build()
          .name("persistent_worker_phase_seconds")
          .labelNames("phase")
          .buckets(TIME_BUCKETS)
          .help("Wall time spent in bounded persistent-worker request and lifecycle phases.")
          .register();
  private static final Histogram poolWaitSeconds =
      Histogram.build()
          .name("persistent_worker_pool_wait_seconds")
          .buckets(TIME_BUCKETS)
          .help("Wall time spent obtaining a process from the persistent-worker keyed pool.")
          .register();
  private static final Counter inputFiles =
      Counter.build()
          .name("persistent_worker_input_files_total")
          .labelNames("method")
          .help("Persistent-worker input files processed by bounded materialization method.")
          .register();
  private static final Counter inputBytes =
      Counter.build()
          .name("persistent_worker_input_bytes_total")
          .labelNames("method")
          .help("Persistent-worker input bytes processed by bounded materialization method.")
          .register();
  private static final Gauge processes =
      Gauge.build()
          .name("persistent_worker_processes")
          .labelNames("state")
          .help("Live persistent-worker processes by bounded pool state.")
          .register();
  private static final Gauge activeKeys =
      Gauge.build()
          .name("persistent_worker_active_keys")
          .help("Distinct worker keys with at least one live persistent-worker process.")
          .register();
  private static final Gauge requestsInFlight =
      Gauge.build()
          .name("persistent_worker_requests_in_flight")
          .help("Persistent-worker requests currently in progress, including pool waiters.")
          .register();
  private static final Gauge poolWaiters =
      Gauge.build()
          .name("persistent_worker_pool_waiters")
          .help("Persistent-worker requests currently waiting for the keyed process pool.")
          .register();
  private static final Counter processStarts =
      Counter.build()
          .name("persistent_worker_process_starts_total")
          .help("Persistent-worker processes started successfully.")
          .register();
  private static final Counter processDestroys =
      Counter.build()
          .name("persistent_worker_process_destroys_total")
          .labelNames("reason")
          .help("Persistent-worker processes destroyed by bounded reason.")
          .register();
  private static final Histogram requestsPerProcess =
      Histogram.build()
          .name("persistent_worker_requests_per_process")
          .buckets(0, 1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 5000)
          .help("Requests handled by a persistent-worker process when it is destroyed.")
          .register();
  private static final Histogram processLifetimeSeconds =
      Histogram.build()
          .name("persistent_worker_process_lifetime_seconds")
          .buckets(1, 5, 10, 30, 60, 300, 900, 3600, 10800, 21600, 43200, 86400)
          .help("Lifetime of a persistent-worker process when it is destroyed.")
          .register();

  private enum State {
    NEW,
    IDLE,
    BUSY
  }

  private static final class WorkerStats {
    private final WorkerKey key;
    private final long startedNanos = System.nanoTime();
    private State state = State.NEW;
    private long requestCount;
    private String destroyReason = DESTROY_POOL;

    private WorkerStats(WorkerKey key) {
      this.key = key;
    }
  }

  private static final ConcurrentHashMap<PersistentWorker, WorkerStats> workerStats =
      new ConcurrentHashMap<>();
  private static final ConcurrentHashMap<WorkerKey, AtomicInteger> workersPerKey =
      new ConcurrentHashMap<>();

  static {
    // Ensure both bounded state series exist even before the first process starts.
    processes.labels("idle");
    processes.labels("busy");
  }

  private PersistentWorkerMetrics() {}

  static long startTimer() {
    return System.nanoTime();
  }

  static void observeRequest(String outcome, long startedNanos) {
    requests.labels(outcome).inc();
    requestSeconds.labels(outcome).observe(elapsedSeconds(startedNanos));
  }

  static void observePhase(String phase, long startedNanos) {
    double elapsed = elapsedSeconds(startedNanos);
    phaseSeconds.labels(phase).observe(elapsed);
    if (phase.equals(PHASE_POOL_WAIT)) {
      poolWaitSeconds.observe(elapsed);
    }
  }

  static void recordInputs(String method, long files, long bytes) {
    inputFiles.labels(method).inc(Math.max(0, files));
    inputBytes.labels(method).inc(Math.max(0, bytes));
  }

  static void requestStarted() {
    requestsInFlight.inc();
  }

  static void requestFinished() {
    requestsInFlight.dec();
  }

  static void poolWaitStarted() {
    poolWaiters.inc();
  }

  static void poolWaitFinished() {
    poolWaiters.dec();
  }

  static void workerStarted(PersistentWorker worker) {
    WorkerKey key = worker.getKey();
    if (workerStats.putIfAbsent(worker, new WorkerStats(key)) == null) {
      processStarts.inc();
      if (key != null) {
        workersPerKey.compute(
            key,
            (ignored, count) -> {
              if (count == null) {
                activeKeys.inc();
                return new AtomicInteger(1);
              }
              count.incrementAndGet();
              return count;
            });
      }
    }
  }

  static void workerBorrowed(PersistentWorker worker) {
    WorkerStats stats = workerStats.get(worker);
    if (stats == null) {
      return;
    }
    synchronized (stats) {
      if (stats.state == State.IDLE) {
        processes.labels("idle").dec();
      } else if (stats.state == State.BUSY) {
        return;
      }
      stats.state = State.BUSY;
      stats.requestCount++;
      processes.labels("busy").inc();
    }
  }

  static void workerReturned(PersistentWorker worker) {
    WorkerStats stats = workerStats.get(worker);
    if (stats == null) {
      return;
    }
    synchronized (stats) {
      if (stats.state != State.BUSY) {
        return;
      }
      processes.labels("busy").dec();
      processes.labels("idle").inc();
      stats.state = State.IDLE;
    }
  }

  static void markDestroyReason(PersistentWorker worker, String reason) {
    WorkerStats stats = workerStats.get(worker);
    if (stats != null) {
      synchronized (stats) {
        stats.destroyReason = reason;
      }
    }
  }

  static void workerDestroyed(PersistentWorker worker) {
    WorkerStats stats = workerStats.remove(worker);
    if (stats == null) {
      return;
    }
    synchronized (stats) {
      if (stats.state == State.IDLE) {
        processes.labels("idle").dec();
      } else if (stats.state == State.BUSY) {
        processes.labels("busy").dec();
      }
      processDestroys.labels(stats.destroyReason).inc();
      requestsPerProcess.observe(stats.requestCount);
      processLifetimeSeconds.observe(elapsedSeconds(stats.startedNanos));
    }
    if (stats.key != null) {
      workersPerKey.computeIfPresent(
          stats.key,
          (ignored, count) -> {
            if (count.decrementAndGet() == 0) {
              activeKeys.dec();
              return null;
            }
            return count;
          });
    }
  }

  private static double elapsedSeconds(long startedNanos) {
    return (System.nanoTime() - startedNanos) / (double) TimeUnit.SECONDS.toNanos(1);
  }
}
