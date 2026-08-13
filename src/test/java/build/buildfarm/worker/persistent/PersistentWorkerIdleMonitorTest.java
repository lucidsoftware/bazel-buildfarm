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

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.mock;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import persistent.bazel.client.PersistentWorker;

@RunWith(JUnit4.class)
public class PersistentWorkerIdleMonitorTest {
  @Test
  public void reportsWorkerThatRemainsIdle() throws Exception {
    PersistentWorkerLifecycle lifecycle = new PersistentWorkerLifecycle();
    PersistentWorker worker = mock(PersistentWorker.class);
    lifecycle.register(worker);
    CountDownLatch candidate = new CountDownLatch(1);

    try (PersistentWorkerIdleMonitor monitor =
        new PersistentWorkerIdleMonitor(
            lifecycle,
            Duration.ofMillis(10),
            (ignoredWorker, ignoredSnapshot) -> candidate.countDown())) {
      monitor.onIdle(worker);

      assertThat(candidate.await(2, TimeUnit.SECONDS)).isTrue();
      assertThat(lifecycle.snapshot(worker).orElseThrow().state())
          .isEqualTo(PersistentWorkerLifecycle.State.IDLE);
    }
  }

  @Test
  public void staleIdleGenerationIsIgnoredAfterWorkerIsReused() {
    PersistentWorkerLifecycle lifecycle = new PersistentWorkerLifecycle();
    PersistentWorker worker = mock(PersistentWorker.class);
    lifecycle.register(worker);
    AtomicInteger candidates = new AtomicInteger();

    try (PersistentWorkerIdleMonitor monitor =
        new PersistentWorkerIdleMonitor(
            lifecycle,
            Duration.ofNanos(1),
            (ignoredWorker, ignoredSnapshot) -> candidates.incrementAndGet())) {
      PersistentWorkerLifecycle.Lease lease = lifecycle.lease(worker, "request");
      assertThat(lifecycle.release(lease)).isTrue();

      monitor.considerCandidate(worker, 0);

      assertThat(candidates.get()).isEqualTo(0);
      assertThat(lifecycle.snapshot(worker).orElseThrow().state())
          .isEqualTo(PersistentWorkerLifecycle.State.IDLE);
    }
  }

  @Test
  public void leasedWorkerIsNotAnIdleCandidate() {
    PersistentWorkerLifecycle lifecycle = new PersistentWorkerLifecycle();
    PersistentWorker worker = mock(PersistentWorker.class);
    lifecycle.register(worker);
    AtomicInteger candidates = new AtomicInteger();

    try (PersistentWorkerIdleMonitor monitor =
        new PersistentWorkerIdleMonitor(
            lifecycle,
            Duration.ofNanos(1),
            (ignoredWorker, ignoredSnapshot) -> candidates.incrementAndGet())) {
      PersistentWorkerLifecycle.Lease lease = lifecycle.lease(worker, "request");

      monitor.considerCandidate(worker, lease.generation());

      assertThat(candidates.get()).isEqualTo(0);
      assertThat(lifecycle.snapshot(worker).orElseThrow().state())
          .isEqualTo(PersistentWorkerLifecycle.State.LEASED);
    }
  }
}
