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

import java.time.Duration;
import java.util.Optional;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.EvictionConfig;
import org.apache.commons.pool2.impl.EvictionPolicy;
import persistent.bazel.client.PersistentWorker;

/** Evicts only objects that both Commons Pool and the lifecycle registry agree are idle. */
final class PersistentWorkerEvictionPolicy implements EvictionPolicy<PersistentWorker> {
  private final PersistentWorkerLifecycle lifecycle;
  private final Duration idleTimeout;
  private final int warmIdleWorkersPerKey;

  PersistentWorkerEvictionPolicy(
      PersistentWorkerLifecycle lifecycle, Duration idleTimeout, int warmIdleWorkersPerKey) {
    this.lifecycle = lifecycle;
    this.idleTimeout = idleTimeout;
    this.warmIdleWorkersPerKey = warmIdleWorkersPerKey;
  }

  @Override
  public boolean evict(
      EvictionConfig config, PooledObject<PersistentWorker> underTest, int idleCount) {
    if (idleCount <= warmIdleWorkersPerKey) {
      return false;
    }
    PersistentWorker worker = underTest.getObject();
    Optional<PersistentWorkerLifecycle.Snapshot> maybeSnapshot = lifecycle.snapshot(worker);
    if (maybeSnapshot.isEmpty()) {
      return false;
    }
    PersistentWorkerLifecycle.Snapshot snapshot = maybeSnapshot.get();
    if (snapshot.state() != PersistentWorkerLifecycle.State.IDLE
        || snapshot.idleDuration().compareTo(idleTimeout) < 0) {
      return false;
    }
    if (!lifecycle.beginRetiringIdle(worker, snapshot.generation())) {
      PersistentWorkerMetrics.staleLifecycleCallback("idle_timeout");
      return false;
    }
    PersistentWorkerMetrics.idleCandidate("enabled");
    PersistentWorkerMetrics.markDestroyReason(worker, PersistentWorkerMetrics.DESTROY_IDLE_TIMEOUT);
    return true;
  }
}
