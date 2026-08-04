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

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.prometheus.client.CollectorRegistry;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import persistent.bazel.client.PersistentWorker;
import persistent.bazel.client.WorkerKey;

@RunWith(JUnit4.class)
public class PersistentWorkerMetricsTest {
  @Test
  public void requestAndInputMetricsHaveBoundedLabels() {
    double requestsBefore =
        sample(
            "persistent_worker_requests_total",
            List.of("outcome"),
            List.of(PersistentWorkerMetrics.OUTCOME_SUCCESS));
    double requestDurationsBefore =
        sample(
            "persistent_worker_request_seconds_count",
            List.of("outcome"),
            List.of(PersistentWorkerMetrics.OUTCOME_SUCCESS));
    double inputFilesBefore =
        sample(
            "persistent_worker_input_files_total",
            List.of("method"),
            List.of(PersistentWorkerMetrics.METHOD_COPY));
    double inputBytesBefore =
        sample(
            "persistent_worker_input_bytes_total",
            List.of("method"),
            List.of(PersistentWorkerMetrics.METHOD_COPY));
    double phasesBefore =
        sample(
            "persistent_worker_phase_seconds_count",
            List.of("phase"),
            List.of(PersistentWorkerMetrics.PHASE_POOL_WAIT));
    double poolWaitBefore = sample("persistent_worker_pool_wait_seconds_count");

    long requestStarted = PersistentWorkerMetrics.startTimer();
    PersistentWorkerMetrics.recordInputs(PersistentWorkerMetrics.METHOD_COPY, 1, 123);
    PersistentWorkerMetrics.observePhase(
        PersistentWorkerMetrics.PHASE_POOL_WAIT, PersistentWorkerMetrics.startTimer());
    PersistentWorkerMetrics.observeRequest(
        PersistentWorkerMetrics.OUTCOME_SUCCESS, requestStarted);

    assertThat(
            sample(
                "persistent_worker_requests_total",
                List.of("outcome"),
                List.of(PersistentWorkerMetrics.OUTCOME_SUCCESS)))
        .isEqualTo(requestsBefore + 1);
    assertThat(
            sample(
                "persistent_worker_request_seconds_count",
                List.of("outcome"),
                List.of(PersistentWorkerMetrics.OUTCOME_SUCCESS)))
        .isEqualTo(requestDurationsBefore + 1);
    assertThat(
            sample(
                "persistent_worker_input_files_total",
                List.of("method"),
                List.of(PersistentWorkerMetrics.METHOD_COPY)))
        .isEqualTo(inputFilesBefore + 1);
    assertThat(
            sample(
                "persistent_worker_input_bytes_total",
                List.of("method"),
                List.of(PersistentWorkerMetrics.METHOD_COPY)))
        .isEqualTo(inputBytesBefore + 123);
    assertThat(
            sample(
                "persistent_worker_phase_seconds_count",
                List.of("phase"),
                List.of(PersistentWorkerMetrics.PHASE_POOL_WAIT)))
        .isEqualTo(phasesBefore + 1);
    assertThat(sample("persistent_worker_pool_wait_seconds_count"))
        .isEqualTo(poolWaitBefore + 1);
  }

  @Test
  public void processLifecycleTracksReuseAndBoundedDestroyReason() {
    PersistentWorker worker = mock(PersistentWorker.class);
    when(worker.getKey()).thenReturn(mock(WorkerKey.class));
    double startsBefore = sample("persistent_worker_process_starts_total");
    double destroysBefore =
        sample(
            "persistent_worker_process_destroys_total",
            List.of("reason"),
            List.of(PersistentWorkerMetrics.DESTROY_TIMEOUT));
    double idleBefore =
        sample("persistent_worker_processes", List.of("state"), List.of("idle"));
    double busyBefore =
        sample("persistent_worker_processes", List.of("state"), List.of("busy"));
    double requestsPerProcessBefore = sample("persistent_worker_requests_per_process_count");
    double activeKeysBefore = sample("persistent_worker_active_keys");

    PersistentWorkerMetrics.workerStarted(worker);
    assertThat(sample("persistent_worker_active_keys")).isEqualTo(activeKeysBefore + 1);
    PersistentWorkerMetrics.workerBorrowed(worker);
    assertThat(sample("persistent_worker_processes", List.of("state"), List.of("busy")))
        .isEqualTo(busyBefore + 1);

    PersistentWorkerMetrics.workerReturned(worker);
    assertThat(sample("persistent_worker_processes", List.of("state"), List.of("idle")))
        .isEqualTo(idleBefore + 1);

    PersistentWorkerMetrics.workerBorrowed(worker);
    PersistentWorkerMetrics.markDestroyReason(worker, PersistentWorkerMetrics.DESTROY_TIMEOUT);
    PersistentWorkerMetrics.workerDestroyed(worker);

    assertThat(sample("persistent_worker_process_starts_total")).isEqualTo(startsBefore + 1);
    assertThat(
            sample(
                "persistent_worker_process_destroys_total",
                List.of("reason"),
                List.of(PersistentWorkerMetrics.DESTROY_TIMEOUT)))
        .isEqualTo(destroysBefore + 1);
    assertThat(sample("persistent_worker_processes", List.of("state"), List.of("idle")))
        .isEqualTo(idleBefore);
    assertThat(sample("persistent_worker_processes", List.of("state"), List.of("busy")))
        .isEqualTo(busyBefore);
    assertThat(sample("persistent_worker_requests_per_process_count"))
        .isEqualTo(requestsPerProcessBefore + 1);
    assertThat(sample("persistent_worker_active_keys")).isEqualTo(activeKeysBefore);
  }

  private static double sample(String name) {
    return sample(name, List.of(), List.of());
  }

  private static double sample(String name, List<String> labelNames, List<String> labelValues) {
    Double value =
        CollectorRegistry.defaultRegistry.getSampleValue(
            name, labelNames.toArray(new String[0]), labelValues.toArray(new String[0]));
    return value == null ? 0 : value;
  }
}
