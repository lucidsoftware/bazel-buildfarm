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

package build.buildfarm.common.config;

import java.util.HashMap;
import java.util.Map;
import lombok.Data;

/** Resource expectations and process-startup overrides for one persistent-worker kind. */
@Data
public class PersistentWorkerProfile {
  /** Stable profile identity; included in the persistent process environment and worker key. */
  private String name = "";

  /** Exact internal execution name, such as {@code Scalac}. */
  private String executionName = "";

  /** Conservative planning estimate for total process RSS; not a runtime-enforced limit. */
  private long estimatedResidentMemoryBytes = 0;

  /** Authoritative overrides applied only when starting the persistent process. */
  private Map<String, String> environment = new HashMap<>();
}
