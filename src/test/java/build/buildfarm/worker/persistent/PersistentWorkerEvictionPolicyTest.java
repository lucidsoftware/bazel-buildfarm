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
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.pool2.PooledObject;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import persistent.bazel.client.CommonsWorkerPool;
import persistent.bazel.client.PersistentWorker;
import persistent.bazel.client.WorkerKey;
import persistent.bazel.client.WorkerSupervisor;

@RunWith(JUnit4.class)
public class PersistentWorkerEvictionPolicyTest {
  @Test
  public void commonsPoolNeverOffersBorrowedWorkerForEviction() throws Exception {
    PersistentWorkerLifecycle lifecycle = new PersistentWorkerLifecycle();
    PersistentWorker worker = mock(PersistentWorker.class);
    WorkerKey key = mock(WorkerKey.class);
    AtomicInteger destroys = new AtomicInteger();
    WorkerSupervisor supervisor =
        new WorkerSupervisor() {
          @Override
          public PersistentWorker create(WorkerKey ignored) {
            lifecycle.register(worker);
            return worker;
          }

          @Override
          public boolean validateObject(
              WorkerKey ignored, PooledObject<PersistentWorker> ignoredPooled) {
            return true;
          }

          @Override
          public void destroyObject(
              WorkerKey ignored, PooledObject<PersistentWorker> ignoredPooled) {
            lifecycle.beginRetiring(worker);
            lifecycle.terminated(worker);
            destroys.incrementAndGet();
          }
        };
    CommonsWorkerPool pool =
        new CommonsWorkerPool(
            supervisor,
            1,
            1,
            0,
            Duration.ofHours(1),
            new PersistentWorkerEvictionPolicy(lifecycle, Duration.ofNanos(1), 0));

    try {
      pool.addObject(key);
      PersistentWorker borrowed = pool.obtain(key);
      PersistentWorkerLifecycle.Lease lease = lifecycle.lease(borrowed, "request");

      pool.evict();

      assertThat(destroys.get()).isEqualTo(0);
      assertThat(lifecycle.snapshot(worker).orElseThrow().state())
          .isEqualTo(PersistentWorkerLifecycle.State.LEASED);

      assertThat(lifecycle.release(lease)).isTrue();
      pool.release(key, borrowed);
      pool.evict();

      assertThat(destroys.get()).isEqualTo(1);
      assertThat(lifecycle.snapshot(worker)).isEmpty();
    } finally {
      pool.close();
    }
  }
}
