/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.aurora.scheduler.storage.log.kv.zookeeper;


import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.aurora.common.zookeeper.testing.ZooKeeperTestServer;
import org.apache.aurora.scheduler.storage.log.kv.AbstractStreamManagerTest;
import org.apache.aurora.scheduler.storage.log.kv.KeyValueStore;
import org.apache.aurora.scheduler.storage.log.kv.KeyValueStore.ListableWithDelete;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.junit.After;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

public class ZooKeeperKeyedStreamManagerTest extends AbstractStreamManagerTest {

  // Hide known container paths ('/' and hash prefix dirs).
  private static final Predicate<String> VISIBLE_KEY =
      key -> (!"/".equals(key)) && key.length() != 3;

  private ZooKeeperTestServer zkTestServer;
  private CuratorFramework curator;

  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();

  @Override
  protected ListableWithDelete<String, byte[]> createStorage() throws Exception {
    zkTestServer = new ZooKeeperTestServer(tmpFolder.newFolder(), tmpFolder.newFolder());
    zkTestServer.startNetwork();

    curator = CuratorFrameworkFactory.builder()
        .namespace("kvstore")
        .dontUseContainerParents() // Container nodes are only available in ZK 3.5+.
        .retryPolicy((retryCount, elapsedTimeMs, sleeper) -> false) // Don't retry.
        .connectString(String.format("localhost:%d", zkTestServer.getPort()))
        .build();
    curator.start();

    return new ZooKeeperKeyValueStore(curator, VISIBLE_KEY);
  }

  @Override
  protected Map<String, byte[]> storeContents(KeyValueStore<String, byte[]> store) {
    if (store instanceof ZooKeeperKeyValueStore) {
      ZooKeeperKeyValueStore zkStore = (ZooKeeperKeyValueStore) store;
      return zkStore.walk("/", 2)
          .filter(VISIBLE_KEY)
          .collect(Collectors.toMap(
              key -> key,
              key -> store.get(key).get()
          ));
    }
    throw new IllegalStateException();
  }

  @After
  public void tearDown() {
    if (curator != null) {
      curator.close();
    }
    if (zkTestServer != null) {
      zkTestServer.stop();
    }
  }
}
