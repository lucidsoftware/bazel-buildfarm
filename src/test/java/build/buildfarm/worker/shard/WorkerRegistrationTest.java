// Copyright 2026 The Buildfarm Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package build.buildfarm.worker.shard;

import static com.google.common.truth.Truth.assertThat;

import io.grpc.Status;
import io.grpc.health.v1.HealthCheckResponse.ServingStatus;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

public class WorkerRegistrationTest {
  @Test
  public void healthIsServingOnlyAfterStorageAndRegistrationAreReady() {
    assertThat(Worker.registrationAwareHealth(/* storageReady= */ false, /* registered= */ false))
        .isEqualTo(ServingStatus.NOT_SERVING);
    assertThat(Worker.registrationAwareHealth(/* storageReady= */ true, /* registered= */ false))
        .isEqualTo(ServingStatus.NOT_SERVING);
    assertThat(Worker.registrationAwareHealth(/* storageReady= */ false, /* registered= */ true))
        .isEqualTo(ServingStatus.NOT_SERVING);
    assertThat(Worker.registrationAwareHealth(/* storageReady= */ true, /* registered= */ true))
        .isEqualTo(ServingStatus.SERVING);
  }

  @Test
  public void retriesOnlyTransientRegistrationFailures() {
    assertThat(
            Worker.isRetryableRegistrationFailure(
                Status.UNAVAILABLE.asRuntimeException()))
        .isTrue();
    assertThat(
            Worker.isRetryableRegistrationFailure(
                Status.DEADLINE_EXCEEDED.asRuntimeException()))
        .isTrue();
    assertThat(
            Worker.isRetryableRegistrationFailure(
                Status.INVALID_ARGUMENT.asRuntimeException()))
        .isFalse();
  }

  @Test
  public void retryBackoffIsBounded() {
    assertThat(Worker.registrationBackoffUpperBoundSeconds(1)).isEqualTo(2);
    assertThat(Worker.registrationBackoffUpperBoundSeconds(4)).isEqualTo(16);
    assertThat(Worker.registrationBackoffUpperBoundSeconds(5)).isEqualTo(30);
    assertThat(Worker.registrationBackoffUpperBoundSeconds(100)).isEqualTo(30);
  }

  @Test
  public void staleWorkerIsRemovedOnlyBeforeFirstRegistration() {
    AtomicInteger removals = new AtomicInteger();

    boolean removalPending =
        Worker.removeStaleWorkerBeforeFirstRegistration(
            /* removalPending= */ true, removals::incrementAndGet);
    assertThat(removalPending).isFalse();
    assertThat(removals.get()).isEqualTo(1);

    removalPending =
        Worker.removeStaleWorkerBeforeFirstRegistration(removalPending, removals::incrementAndGet);
    assertThat(removalPending).isFalse();
    assertThat(removals.get()).isEqualTo(1);
  }
}
