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

package persistent.common.processes;

import static org.junit.Assert.assertThrows;

import java.time.Duration;
import org.apache.commons.pool2.BaseKeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import persistent.common.CommonsPool;
import persistent.common.PoolExhaustedException;

@RunWith(JUnit4.class)
public class CommonsPoolTest {
  @Test
  public void boundedBorrowReportsExhaustion() throws Exception {
    CommonsPool<String, Object> pool =
        new CommonsPool<>(
            new BaseKeyedPooledObjectFactory<>() {
              @Override
              public Object create(String key) {
                return new Object();
              }

              @Override
              public PooledObject<Object> wrap(Object value) {
                return new DefaultPooledObject<>(value);
              }
            },
            1);
    Object borrowed = pool.borrowObject("key", Duration.ZERO);
    try {
      assertThrows(
          PoolExhaustedException.class, () -> pool.borrowObject("key", Duration.ofMillis(25)));
    } finally {
      pool.returnObject("key", borrowed);
      pool.close();
    }
  }
}
