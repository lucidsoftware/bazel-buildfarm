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

import build.buildfarm.common.config.PersistentWorkerProfile;
import build.buildfarm.common.config.PersistentWorkers;
import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/** Resolves bounded configuration profiles without changing an action's original environment. */
final class PersistentWorkerProfileResolver {
  static final String PROFILE_ENVIRONMENT_VARIABLE = "BUILDFARM_PERSISTENT_WORKER_PROFILE";

  private PersistentWorkerProfileResolver() {}

  static Optional<PersistentWorkerProfile> resolve(
      PersistentWorkers settings, String executionName) {
    return settings.getProfiles().stream()
        .filter(profile -> profile.getExecutionName().equals(executionName))
        .findFirst();
  }

  static ImmutableMap<String, String> applyEnvironment(
      PersistentWorkerProfile profile, Map<String, String> actionEnvironment) {
    Map<String, String> profiledEnvironment = new HashMap<>(actionEnvironment);
    profiledEnvironment.putAll(profile.getEnvironment());
    profiledEnvironment.put(PROFILE_ENVIRONMENT_VARIABLE, profile.getName());
    return ImmutableMap.copyOf(profiledEnvironment);
  }
}
