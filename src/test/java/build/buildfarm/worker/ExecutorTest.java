// Copyright 2026 The Buildfarm Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package build.buildfarm.worker;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import build.buildfarm.common.Claim;
import com.google.common.collect.ImmutableList;
import com.google.devtools.build.lib.worker.WorkerProtocol.WorkResponse;
import com.google.longrunning.Operation;
import com.google.protobuf.Duration;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Test;
import persistent.bazel.client.WorkerResources;

public class ExecutorTest {
  @Test
  public void persistentWorkerAllowsMarkedExecutableForAllowlistedMnemonic() {
    assertThat(
            Executor.isPersistentWorkerEligible(
                "ScalaCompile", ImmutableList.of("ScalaCompile"), true))
        .isTrue();
  }

  @Test
  public void persistentWorkerAllowsMarkedExecutableForWildcardAllowlist() {
    assertThat(Executor.isPersistentWorkerEligible("ScalaCompile", ImmutableList.of("*"), true))
        .isTrue();
  }

  @Test
  public void persistentWorkerRejectsUnmarkedExecutable() {
    assertThat(
            Executor.isPersistentWorkerEligible(
                "ScalaCompile", ImmutableList.of("ScalaCompile"), false))
        .isFalse();
  }

  @Test
  public void persistentWorkerRejectsMnemonicOutsideAllowlist() {
    assertThat(
            Executor.isPersistentWorkerEligible(
                "TsProject", ImmutableList.of("ScalaCompile"), true))
        .isFalse();
  }

  @Test
  public void persistentRequestReportsOnlyItsOwnCpuAndAppliesLeaseBeforeResume() throws Exception {
    Claim claim = mock(Claim.class);
    CPULease lease = mock(CPULease.class);
    when(claim.get(CPULease.RESOURCE_NAME)).thenReturn(lease);
    when(lease.amount()).thenReturn(2000);
    ExecutionContext context =
        ExecutionContext.newBuilder()
            .setOperation(Operation.newBuilder().setName("request").build())
            .setClaim(claim)
            .build();
    Executor executor =
        new Executor(
            mock(WorkerContext.class), context, null, 100, 20, 20, 100, 1000, Runnable::run);
    AtomicLong cumulative = new AtomicLong(1000000);
    AtomicLong quota = new AtomicLong();
    WorkerResources resources =
        new WorkerResources() {
          public void setCpu(int micros) {
            quota.set(micros);
          }

          public void resume() {
            assertThat(quota.get()).isEqualTo(200000);
          }

          public Map<String, Long> sample() {
            return Map.of(
                "cpu.usage_usec",
                cumulative.get(),
                "cpu.nr_periods",
                cumulative.get() / 100,
                "cpu.throttled_usec",
                0L);
          }
        };
    for (int i = 0; i < 2; i++) {
      executor.executePersistentRequest(
          resources,
          () -> {
            cumulative.addAndGet(500);
            return WorkResponse.newBuilder().setOutput("done").build();
          },
          Duration.newBuilder().setSeconds(5).build());
      assertThat(context.workerExecutedMetadata.getUsageOrThrow("cpu.usage_usec")).isEqualTo(500);
    }
  }

  @Test
  public void persistentRequestTimeoutCancelsItsResponseTask() throws Exception {
    Claim claim = mock(Claim.class);
    CPULease lease = mock(CPULease.class);
    when(claim.get(CPULease.RESOURCE_NAME)).thenReturn(lease);
    when(lease.amount()).thenReturn(1000);
    ExecutionContext context =
        ExecutionContext.newBuilder()
            .setOperation(Operation.newBuilder().setName("timeout").build())
            .setClaim(claim)
            .build();
    Executor executor =
        new Executor(
            mock(WorkerContext.class), context, null, 100, 20, 20, 100, 1000, Runnable::run);
    CountDownLatch exited = new CountDownLatch(1);
    assertThrows(
        TimeoutException.class,
        () ->
            executor.executePersistentRequest(
                WorkerResources.NONE,
                () -> {
                  try {
                    new CountDownLatch(1).await();
                    return WorkResponse.getDefaultInstance();
                  } finally {
                    exited.countDown();
                  }
                },
                Duration.newBuilder().setNanos(100000000).build()));
    assertThat(exited.await(2, TimeUnit.SECONDS)).isTrue();
  }

  @Test
  public void persistentRequestReturnsUnusedCpuThroughTheSharedMarketLoop() throws Exception {
    Claim claim = mock(Claim.class);
    CPULease lease = mock(CPULease.class);
    Market market = mock(Market.class);
    WorkerContext workerContext = mock(WorkerContext.class);
    when(workerContext.market()).thenReturn(market);
    when(claim.get(CPULease.RESOURCE_NAME)).thenReturn(lease);
    AtomicInteger balance = new AtomicInteger(2000);
    when(lease.amount()).thenAnswer(invocation -> balance.get());
    doAnswer(
            invocation -> {
              balance.addAndGet(-(int) invocation.getArgument(0));
              return null;
            })
        .when(lease)
        .deplete(1000);
    ExecutionContext context =
        ExecutionContext.newBuilder()
            .setMarketExecution(true)
            .setOperation(Operation.newBuilder().setName("market").build())
            .setClaim(claim)
            .build();
    Executor executor =
        new Executor(workerContext, context, null, 100, 20, 20, 100, 1000, Runnable::run);
    CountDownLatch reduced = new CountDownLatch(1);
    AtomicLong samples = new AtomicLong();
    WorkerResources resources =
        new WorkerResources() {
          public void setCpu(int micros) {
            if (micros == 100000) {
              reduced.countDown();
            }
          }

          public Map<String, Long> sample() {
            return Map.of(
                "cpu.usage_usec",
                0L,
                "cpu.nr_periods",
                samples.incrementAndGet(),
                "cpu.throttled_usec",
                0L);
          }
        };
    executor.executePersistentRequest(
        resources,
        () -> {
          assertThat(reduced.await(2, TimeUnit.SECONDS)).isTrue();
          return WorkResponse.getDefaultInstance();
        },
        Duration.newBuilder().setSeconds(5).build());
    verify(market).sell(1000);
    assertThat(balance.get()).isEqualTo(1000);
  }
}
