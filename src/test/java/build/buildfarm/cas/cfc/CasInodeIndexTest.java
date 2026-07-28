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

import static com.google.common.truth.Truth.assertThat;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertThrows;

import build.buildfarm.cas.cfc.CASFileCache.Entry;
import io.grpc.Deadline;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class CasInodeIndexTest {
  private static Entry entry(String key) {
    return Entry.orphan(key, /* size= */ 1, Deadline.after(10, HOURS));
  }

  @Test
  public void increment_zeroToOne_insertsIntoIndex() {
    CasInodeIndex index = new CasInodeIndex();
    Entry e = entry("a");
    Object fileKey = new Object();

    index.increment(e, fileKey);

    assertThat(e.casDirectoryHardlinkCount()).isEqualTo(1);
    assertThat(index.get(fileKey)).isSameInstanceAs(e);
    assertThat(index.size()).isEqualTo(1);
  }

  @Test
  public void increment_oneToMany_keepsSingleIndexEntry() {
    CasInodeIndex index = new CasInodeIndex();
    Entry e = entry("a");
    Object fileKey = new Object();

    for (int i = 0; i < 10; i++) {
      index.increment(e, fileKey);
    }

    assertThat(e.casDirectoryHardlinkCount()).isEqualTo(10);
    assertThat(index.size()).isEqualTo(1);
    assertThat(index.get(fileKey)).isSameInstanceAs(e);
  }

  @Test
  public void decrement_oneToZero_removesFromIndex() {
    CasInodeIndex index = new CasInodeIndex();
    Entry e = entry("a");
    Object fileKey = new Object();
    index.increment(e, fileKey);

    assertThat(index.decrement(e, fileKey)).isEqualTo(0);

    assertThat(e.casDirectoryHardlinkCount()).isEqualTo(0);
    assertThat(index.get(fileKey)).isNull();
    assertThat(index.size()).isEqualTo(0);
  }

  @Test
  public void decrement_aboveOne_keepsIndexEntry() {
    CasInodeIndex index = new CasInodeIndex();
    Entry e = entry("a");
    Object fileKey = new Object();
    index.increment(e, fileKey);
    index.increment(e, fileKey);

    assertThat(index.decrement(e, fileKey)).isEqualTo(1);

    assertThat(e.casDirectoryHardlinkCount()).isEqualTo(1);
    assertThat(index.get(fileKey)).isSameInstanceAs(e);
  }

  @Test
  public void inodeReuse_remapsToNewEntryOnlyAfterPriorOccupantReleased() {
    CasInodeIndex index = new CasInodeIndex();
    Entry oldOccupant = entry("old");
    Entry newOccupant = entry("new");
    Object fileKey = new Object(); // same inode identity, reused across occupants

    index.increment(oldOccupant, fileKey);
    assertThat(index.get(fileKey)).isSameInstanceAs(oldOccupant);

    assertThat(index.decrement(oldOccupant, fileKey)).isEqualTo(0);
    assertThat(index.get(fileKey)).isNull();

    index.increment(newOccupant, fileKey);
    assertThat(index.get(fileKey)).isSameInstanceAs(newOccupant);
    assertThat(newOccupant.casDirectoryHardlinkCount()).isEqualTo(1);
    assertThat(oldOccupant.casDirectoryHardlinkCount()).isEqualTo(0);

    assertThat(index.decrement(newOccupant, fileKey)).isEqualTo(0);
    assertThat(index.get(fileKey)).isNull();
    assertThat(index.size()).isEqualTo(0);
  }

  @Test
  public void decrement_oldEntryDoesNotRemoveNewMappingForSameFileKey() {
    CasInodeIndex index = new CasInodeIndex();
    Entry oldOccupant = entry("old");
    Entry newOccupant = entry("new");
    Object fileKey = new Object();

    index.increment(oldOccupant, fileKey);
    index.increment(newOccupant, fileKey);
    assertThat(index.get(fileKey)).isSameInstanceAs(newOccupant);

    assertThat(index.decrement(oldOccupant, fileKey)).isEqualTo(0);

    assertThat(index.get(fileKey)).isSameInstanceAs(newOccupant);
    assertThat(newOccupant.casDirectoryHardlinkCount()).isEqualTo(1);
    assertThat(oldOccupant.casDirectoryHardlinkCount()).isEqualTo(0);
  }

  @Test
  public void decrement_underflow_failsLoudly() {
    CasInodeIndex index = new CasInodeIndex();
    Entry e = entry("a");
    Object fileKey = new Object();

    assertThrows(IllegalStateException.class, () -> index.decrement(e, fileKey));

    assertThat(e.casDirectoryHardlinkCount()).isEqualTo(0);
  }

  @Test
  public void concurrentBalancedIncrementsDecrements_leaveCountZeroAndMapEmpty() throws Exception {
    CasInodeIndex index = new CasInodeIndex();
    Entry e = entry("a");
    Object fileKey = new Object();

    int threads = 8;
    int iterations = 5000;
    CyclicBarrier start = new CyclicBarrier(threads);
    CountDownLatch done = new CountDownLatch(threads);
    CopyOnWriteArrayList<Throwable> failures = new CopyOnWriteArrayList<>();

    for (int t = 0; t < threads; t++) {
      Thread worker =
          new Thread(
              () -> {
                try {
                  start.await();
                  for (int i = 0; i < iterations; i++) {
                    index.increment(e, fileKey);
                    index.decrement(e, fileKey);
                  }
                } catch (Throwable failure) {
                  failures.add(failure);
                } finally {
                  done.countDown();
                }
              });
      worker.setDaemon(true);
      worker.start();
    }

    assertThat(done.await(30, SECONDS)).isTrue();
    assertThat(failures).isEmpty();
    assertThat(e.casDirectoryHardlinkCount()).isEqualTo(0);
    assertThat(index.size()).isEqualTo(0);
    assertThat(index.get(fileKey)).isNull();
  }

  @Test
  public void distinctInodes_trackIndependently() {
    CasInodeIndex index = new CasInodeIndex();
    Entry a = entry("a");
    Entry b = entry("b");
    Object keyA = new Object();
    Object keyB = new Object();

    index.increment(a, keyA);
    index.increment(b, keyB);

    assertThat(index.size()).isEqualTo(2);
    assertThat(index.get(keyA)).isSameInstanceAs(a);
    assertThat(index.get(keyB)).isSameInstanceAs(b);

    index.decrement(a, keyA);
    assertThat(index.size()).isEqualTo(1);
    assertThat(index.get(keyA)).isNull();
    assertThat(index.get(keyB)).isSameInstanceAs(b);
  }
}
