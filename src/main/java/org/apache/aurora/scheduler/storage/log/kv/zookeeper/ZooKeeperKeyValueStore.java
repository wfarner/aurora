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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.inject.Inject;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;

import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Data;
import org.apache.aurora.scheduler.storage.log.kv.KeyValueStore.ListableWithDelete;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.curator.framework.api.transaction.CuratorTransactionResult;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;

import static java.util.Objects.requireNonNull;

class ZooKeeperKeyValueStore implements ListableWithDelete<String, byte[]> {

  private static final int MAX_TXN_BYTES = Amount.of(512, Data.KB).as(Data.BYTES);

  private final CuratorFramework client;
  private final Predicate<String> visibleKey;

  // A cache to avoid redundant container dir creation.  The boolean value is unused.
  private final LoadingCache<String, Boolean> nodeDirs = CacheBuilder.newBuilder()
      .build(new CacheLoader<String, Boolean>() {
        @Override
        public Boolean load(String key) throws Exception {
          try {
            client.createContainers(key);
            return true;
          } catch (KeeperException.NodeExistsException e) {
            // This may indicate a race of some sort, but is otherwise benign.
            return true;
          } catch (Exception e) {
            throw new StoreException("Failed to create container node " + key, e);
          }
        }
      });

  @Inject
  ZooKeeperKeyValueStore(CuratorFramework client, Predicate<String> visibleKey) {
    this.client = requireNonNull(client);
    this.visibleKey = requireNonNull(visibleKey);
  }

  private Stream<String> walkRecursive(String path, int depth, int maxDepth)
      throws StoreException {

    List<String> children;
    try {
      children = client.getChildren().forPath(path);
    } catch (NoNodeException e) {
      return Stream.empty();
    } catch (Exception e) {
      throw new StoreException("Failed to list " + path, e);
    }

    Stream<String> nodes;
    if (children.isEmpty()) {
      nodes = Stream.of(path);
    } else {
      nodes = children.stream()
          .map(child -> (path.endsWith("/") ? path : (path + "/")) + child);
    }

    if (depth == maxDepth) {
      return nodes;
    }

    return nodes.map(child -> walkRecursive(child, depth + 1, maxDepth))
        .flatMap(Function.identity());
  }

  @VisibleForTesting
  Stream<String> walk(String path, int maxDepth) throws StoreException {
    return walkRecursive(path, 1, maxDepth);
  }

  @Override
  public Set<String> keys() throws StoreException {
    return walk("/", 2)
        .filter(visibleKey)
        .peek(nodePath -> {
          if (nodePath.contains("/")) {
            nodeDirs.asMap().put(nodePath.substring(0, nodePath.lastIndexOf("/")), true);
          }
        })
        .collect(Collectors.toSet());
  }

  @Override
  public Optional<byte[]> get(String key) throws StoreException {
    try {
      return Optional.ofNullable(client.getData().forPath(key));
    } catch (KeeperException.NoNodeException e) {
      return Optional.empty();
    } catch (Exception e) {
      throw new StoreException("Failed to get " + key, e);
    }
  }

  @Override
  public void save(Map<String, Record<byte[]>> records) throws StoreException {
    if (records.isEmpty()) {
      return;
    }

    // Ensure that all parent nodes exist.
    records.keySet().stream()
        .filter(k -> k.contains("/"))
        .map(k -> k.substring(0, k.lastIndexOf("/")))
        .forEach(nodeDirs::getUnchecked);

    AtomicInteger approxTxnBytes = new AtomicInteger(0);
    AtomicReference<List<CuratorOp>> ops = new AtomicReference<>(Lists.newArrayList());
    Runnable commit = () -> {
      List<CuratorOp> txnOps = ops.getAndSet(Lists.newArrayList());
      approxTxnBytes.set(0);
      if (txnOps.isEmpty()) {
        return;
      }

      commitTxn(txnOps, records.keySet());
    };

    records.forEach((path, record) -> {
      byte[] data = record.isTombstone() ? null : record.getValue();

      int entryBytes = path.length() + (data == null ? 0 : data.length);
      Preconditions.checkArgument(
          entryBytes < MAX_TXN_BYTES,
          "Entry for " + path + " exceeds transaction size limit");

      if (approxTxnBytes.get() + entryBytes >= MAX_TXN_BYTES) {
        // Including this entry in the current transaction would exceed the size limit.  Commit
        // it before continuing.
        commit.run();
      } else {
        try {
          // TODO(wfarner): REMOVE HACK
          if ("/last_transaction".equals(path) && initialized.getAndSet(true)) {
            ops.get().add(client.transactionOp().setData()
                .forPath(path, data));
          } else {
            ops.get().add(client.transactionOp().create()
                .withMode(CreateMode.PERSISTENT)
                .forPath(path, data));
          }
        } catch (Exception e) {
          throw new StoreException("Failed to create transaction op for " + path, e);
        }
      }
    });

    commit.run();
  }

  private final AtomicBoolean initialized = new AtomicBoolean(false);

  @Override
  public void delete(Set<String> keys) throws StoreException {
    List<CuratorOp> ops = keys.stream()
        .map(key -> {
          try {
            return client.transactionOp().delete().forPath(key);
          } catch (Exception e) {
            throw new StoreException("Failed to create delete op for " + key, e);
          }
        })
        .collect(Collectors.toList());
    commitTxn(ops, keys);
  }

  private void commitTxn(List<CuratorOp> ops, Object errContext) throws StoreException {
    Collection<CuratorTransactionResult> results;
    try {
      results = client.transaction().forOperations(ops);
    } catch (Exception e) {
      throw new StoreException("ZK transaction failed for " + errContext, e);
    }

    List<CuratorTransactionResult> errors = results.stream()
        .filter(result -> result.getError() != 0)
        .collect(Collectors.toList());
    if (!errors.isEmpty()) {
      throw new StoreException("Transaction returned errors: " + errors);
    }
  }
}
