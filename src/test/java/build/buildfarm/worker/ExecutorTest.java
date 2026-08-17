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
import static org.mockito.Mockito.mock;

import build.buildfarm.worker.persistent.WorkFilesContext;
import com.google.common.collect.ImmutableList;
import com.google.rpc.Code;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;
import persistent.common.PoolExhaustedException;

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
  public void poolExhaustionRetriesThroughOrdinaryExecution() throws Exception {
    WorkFilesContext persistentContext = mock(WorkFilesContext.class);
    List<WorkFilesContext> attempts = new ArrayList<>();

    Code result =
        Executor.executeWithPersistentWorkerFallback(
            "operation",
            persistentContext,
            context -> {
              attempts.add(context);
              if (context != null) {
                throw new PoolExhaustedException("pool exhausted", null);
              }
              return Code.OK;
            });

    assertThat(result).isEqualTo(Code.OK);
    assertThat(attempts).containsExactly(persistentContext, null).inOrder();
  }

  @Test
  public void nonCapacityFailureDoesNotRetryThroughOrdinaryExecution() {
    WorkFilesContext persistentContext = mock(WorkFilesContext.class);
    List<WorkFilesContext> attempts = new ArrayList<>();

    assertThrows(
        IOException.class,
        () ->
            Executor.executeWithPersistentWorkerFallback(
                "operation",
                persistentContext,
                context -> {
                  attempts.add(context);
                  throw new IOException("worker setup failed");
                }));

    assertThat(attempts).containsExactly(persistentContext);
  }
}
