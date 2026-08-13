// Copyright 2026 The Buildfarm Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package build.buildfarm.worker.persistent;

import static com.google.common.base.Preconditions.checkArgument;

import build.buildfarm.common.config.PersistentWorkers;
import java.time.Duration;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.logging.Level;
import lombok.extern.java.Log;
import persistent.bazel.client.PersistentWorker;

/** Reports generation-validated idle workers without changing their lifecycle or pool state. */
@Log
final class PersistentWorkerIdleMonitor implements AutoCloseable {
  private final PersistentWorkerLifecycle lifecycle;
  private final Duration idleTimeout;
  private final BiConsumer<PersistentWorker, PersistentWorkerLifecycle.Snapshot> candidateListener;
  private final ScheduledExecutorService scheduler;
  private final Map<PersistentWorker, ScheduledFuture<?>> candidates = new IdentityHashMap<>();

  static PersistentWorkerIdleMonitor from(
      PersistentWorkers settings, PersistentWorkerLifecycle lifecycle) {
    if (settings.getIdleRetirementMode() != PersistentWorkers.IdleRetirementMode.SHADOW) {
      return new PersistentWorkerIdleMonitor(lifecycle);
    }
    return new PersistentWorkerIdleMonitor(
        lifecycle,
        Duration.ofSeconds(settings.getIdleTimeoutSeconds()),
        (worker, snapshot) ->
            log.info(
                String.format(
                    "Persistent worker is an idle-retirement candidate: mnemonic=%s, generation=%d,"
                        + " idle=%s (shadow mode; no action taken)",
                    worker.getKey().getMnemonic(),
                    snapshot.generation(),
                    snapshot.idleDuration())));
  }

  static PersistentWorkerIdleMonitor disabled(PersistentWorkerLifecycle lifecycle) {
    return new PersistentWorkerIdleMonitor(lifecycle);
  }

  private PersistentWorkerIdleMonitor(PersistentWorkerLifecycle lifecycle) {
    this.lifecycle = lifecycle;
    idleTimeout = Duration.ZERO;
    candidateListener = (worker, snapshot) -> {};
    scheduler = null;
  }

  PersistentWorkerIdleMonitor(
      PersistentWorkerLifecycle lifecycle,
      Duration idleTimeout,
      BiConsumer<PersistentWorker, PersistentWorkerLifecycle.Snapshot> candidateListener) {
    checkArgument(
        !idleTimeout.isZero() && !idleTimeout.isNegative(), "idle timeout must be positive");
    this.lifecycle = lifecycle;
    this.idleTimeout = idleTimeout;
    this.candidateListener = candidateListener;
    ScheduledThreadPoolExecutor executor =
        new ScheduledThreadPoolExecutor(
            1,
            runnable -> {
              Thread thread = new Thread(runnable, "persistent-worker-idle-monitor");
              thread.setDaemon(true);
              return thread;
            });
    executor.setRemoveOnCancelPolicy(true);
    scheduler = executor;
  }

  synchronized void onIdle(PersistentWorker worker) {
    if (scheduler == null) {
      return;
    }
    Optional<PersistentWorkerLifecycle.Snapshot> maybeSnapshot = lifecycle.snapshot(worker);
    if (maybeSnapshot.isEmpty()
        || maybeSnapshot.get().state() != PersistentWorkerLifecycle.State.IDLE) {
      return;
    }
    PersistentWorkerLifecycle.Snapshot snapshot = maybeSnapshot.get();
    cancel(worker);
    long delayNanos = Math.max(0, idleTimeout.minus(snapshot.idleDuration()).toNanos());
    candidates.put(
        worker,
        scheduler.schedule(
            () -> considerCandidate(worker, snapshot.generation()),
            delayNanos,
            TimeUnit.NANOSECONDS));
  }

  synchronized void onUnavailable(PersistentWorker worker) {
    cancel(worker);
  }

  private void cancel(PersistentWorker worker) {
    ScheduledFuture<?> candidate = candidates.remove(worker);
    if (candidate != null) {
      candidate.cancel(false);
    }
  }

  void considerCandidate(PersistentWorker worker, long generation) {
    try {
      Optional<PersistentWorkerLifecycle.Snapshot> maybeSnapshot = lifecycle.snapshot(worker);
      if (maybeSnapshot.isEmpty()) {
        return;
      }
      PersistentWorkerLifecycle.Snapshot snapshot = maybeSnapshot.get();
      if (snapshot.state() != PersistentWorkerLifecycle.State.IDLE
          || snapshot.generation() != generation
          || snapshot.idleDuration().compareTo(idleTimeout) < 0) {
        PersistentWorkerMetrics.staleLifecycleCallback("idle_timeout");
        return;
      }
      synchronized (this) {
        candidates.remove(worker);
      }
      PersistentWorkerMetrics.idleCandidate("shadow");
      candidateListener.accept(worker, snapshot);
    } catch (Throwable t) {
      log.log(Level.SEVERE, "Exception while checking a persistent worker's idle state", t);
    }
  }

  @Override
  public synchronized void close() {
    for (ScheduledFuture<?> candidate : candidates.values()) {
      candidate.cancel(false);
    }
    candidates.clear();
    if (scheduler != null) {
      scheduler.shutdownNow();
    }
  }
}
