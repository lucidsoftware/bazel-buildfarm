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

package build.buildfarm.cas.cfc;

import build.buildfarm.cas.cfc.CASFileCache.Entry;
import io.prometheus.client.Counter;
import java.util.concurrent.ConcurrentHashMap;
import org.jspecify.annotations.Nullable;

/** Maps hardlinked file inodes to their source CAS entries. */
final class CasInodeIndex {
  // Makes hardlink-accounting bugs visible outside logs.
  private static final Counter hardlinkCountUnderflowTotal =
      Counter.build()
          .name("cas_hardlink_count_underflow_total")
          .help("casDirectoryHardlinkCount decrement underflows (accounting-bug canary; expect 0).")
          .register();

  // BasicFileAttributes.fileKey() has no more specific cross-platform type.
  private final ConcurrentHashMap<Object, Entry> map = new ConcurrentHashMap<>();

  /** Increments the hardlink count and indexes the first link. */
  void increment(Entry entry, Object fileKey) {
    int old = entry.getAndAddCasDirectoryHardlinkCount(1);
    if (old == 0) {
      map.compute(
          fileKey, (k, existing) -> entry.casDirectoryHardlinkCount() > 0 ? entry : existing);
    }
  }

  /**
   * Decrements the hardlink count and removes the final link from the index.
   *
   * @return the count produced by this decrement; zero means the caller must wake the evictor
   */
  int decrement(Entry entry, Object fileKey) {
    int old = entry.getAndAddCasDirectoryHardlinkCount(-1);
    if (old <= 0) {
      entry.getAndAddCasDirectoryHardlinkCount(1);
      hardlinkCountUnderflowTotal.inc();
      throw new IllegalStateException(
          "entry " + entry.key + " casDirectoryHardlinkCount underflow (was " + old + ")");
    }
    if (old == 1) {
      map.compute(
          fileKey,
          (k, existing) ->
              existing == entry && entry.casDirectoryHardlinkCount() <= 0 ? null : existing);
    }
    return old - 1;
  }

  @Nullable Entry get(Object fileKey) {
    return map.get(fileKey);
  }

  int size() {
    return map.size();
  }
}
