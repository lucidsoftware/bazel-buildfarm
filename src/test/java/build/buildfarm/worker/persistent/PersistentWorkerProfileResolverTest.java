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

import build.buildfarm.common.config.PersistentWorkerProfile;
import build.buildfarm.common.config.PersistentWorkers;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import org.junit.Test;

public class PersistentWorkerProfileResolverTest {
  @Test
  public void resolveMatchesExactExecutionName() {
    PersistentWorkerProfile profile = profile("scalac", "Scalac");
    PersistentWorkers settings = new PersistentWorkers();
    settings.setProfiles(List.of(profile));

    assertThat(PersistentWorkerProfileResolver.resolve(settings, "Scalac").orElseThrow())
        .isSameInstanceAs(profile);
    assertThat(PersistentWorkerProfileResolver.resolve(settings, "JavaBuilder")).isEmpty();
  }

  @Test
  public void applyEnvironmentOverridesOnlyPersistentWorkerCopy() {
    PersistentWorkerProfile profile = profile("scalac", "Scalac");
    profile.setEnvironment(
        ImmutableMap.of("JAVA_TOOL_OPTIONS", "-Xmx4g", "PROFILE_SETTING", "enabled"));
    ImmutableMap<String, String> actionEnvironment =
        ImmutableMap.of("JAVA_TOOL_OPTIONS", "-Xmx16g", "ACTION_SETTING", "preserved");

    ImmutableMap<String, String> profiled =
        PersistentWorkerProfileResolver.applyEnvironment(profile, actionEnvironment);

    assertThat(profiled)
        .containsExactly(
            "JAVA_TOOL_OPTIONS",
            "-Xmx4g",
            "PROFILE_SETTING",
            "enabled",
            "ACTION_SETTING",
            "preserved",
            PersistentWorkerProfileResolver.PROFILE_ENVIRONMENT_VARIABLE,
            "scalac");
    assertThat(actionEnvironment)
        .containsExactly("JAVA_TOOL_OPTIONS", "-Xmx16g", "ACTION_SETTING", "preserved");
  }

  private static PersistentWorkerProfile profile(String name, String executionName) {
    PersistentWorkerProfile profile = new PersistentWorkerProfile();
    profile.setName(name);
    profile.setExecutionName(executionName);
    profile.setEstimatedResidentMemoryBytes(6L * 1024 * 1024 * 1024);
    return profile;
  }
}
