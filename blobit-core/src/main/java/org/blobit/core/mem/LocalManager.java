/*
 Licensed to Diennea S.r.l. under one
 or more contributor license agreements. See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership. Diennea S.r.l. licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.

 */
package org.blobit.core.mem;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.blobit.core.api.BucketConfiguration;
import org.blobit.core.api.BucketMetadata;
import org.blobit.core.api.LedgerMetadata;
import org.blobit.core.api.ObjectManager;
import org.blobit.core.api.ObjectManagerException;
import org.blobit.core.api.ObjectMetadata;
import org.blobit.core.api.PutPromise;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * MetadataManager all in memory for unit tests
 *
 * @author enrico.olivelli
 */
public class LocalManager implements ObjectManager {

    private final Map<String, MemBucket> buckets = new ConcurrentHashMap<>();

    @Override
    public void createBucket(String name, String bucketTableSpaceName, BucketConfiguration configuration) throws ObjectManagerException {

        MemBucket oldBucket = buckets.computeIfAbsent(name, (bname) -> {
            return new MemBucket(bname, configuration);
        });
        if (oldBucket == null) {
            throw new ObjectManagerException("bucket " + name + " already exists");
        }
    }

    @Override
    public List<BucketMetadata> listBuckets() throws ObjectManagerException {
        return buckets
            .values()
            .stream()
            .map(MemBucket::getMetadata)
            .collect(Collectors.toList());
    }

    private MemBucket getBucket(String bucketId) throws ObjectManagerException {
        MemBucket bucket = buckets.get(bucketId);
        if (bucket == null) {
            throw new ObjectManagerException("bucket " + bucketId + " does not exist");
        }
        return bucket;
    }

    Collection<Long> listDeletableLedgers(String bucketId) throws ObjectManagerException {
        return getBucket(bucketId).listDeletableLedgers();
    }

    Collection<LedgerMetadata> listLedgersbyBucketId(String bucketId) throws ObjectManagerException {
        return getBucket(bucketId).listLedgers();
    }

    Collection<ObjectMetadata> listObjectsByLedger(String bucketId, long ledgerId) throws ObjectManagerException {
        return getBucket(bucketId).getLedger(ledgerId).listObjects();
    }

    @Override
    public void close() {
        buckets.clear();
    }

    @Override
    public PutPromise put(String bucketId, byte[] data) {
        return put(bucketId, data, 0, data.length);
    }

    @Override
    @SuppressFBWarnings("NP_NONNULL_PARAM_VIOLATION")
    public PutPromise put(String bucketId, byte[] data, int offset, int len) {
        try {
            if (offset != 0 && len < data.length) {
                byte[] copy = new byte[len];
                System.arraycopy(data, offset, copy, 0, len);
                data = copy;
            }
            MemEntryId res = getBucket(bucketId).getCurrentLedger().put(data);
            /* NP_NONNULL_PARAM_VIOLATION: https://github.com/findbugsproject/findbugs/issues/79 */
            return new PutPromise(res.toId(), CompletableFuture.<Void>completedFuture(null));
        } catch (ObjectManagerException err) {
            CompletableFuture<Void> res = new CompletableFuture<>();
            res.completeExceptionally(err);
            return new PutPromise(null, res);
        }
    }

    @Override
    @SuppressFBWarnings("NP_NONNULL_PARAM_VIOLATION")
    public CompletableFuture<byte[]> get(String bucketId, String objectId) {
        try {
            MemEntryId id = MemEntryId.parseId(objectId);
            byte[] res = getBucket(bucketId).getLedger(id.ledgerId).get(id.firstEntryId);
            /* NP_NONNULL_PARAM_VIOLATION: https://github.com/findbugsproject/findbugs/issues/79 */
            return CompletableFuture.completedFuture(res);
        } catch (ObjectManagerException err) {
            CompletableFuture<byte[]> res = new CompletableFuture<>();
            res.completeExceptionally(err);
            return res;
        }
    }

    @SuppressFBWarnings("NP_NONNULL_PARAM_VIOLATION")
    @Override
    public CompletableFuture<Void> delete(String bucketId, String objectId) {
        try {
            MemEntryId id = MemEntryId.parseId(objectId);
            getBucket(bucketId).getLedger(id.ledgerId).delete(id.firstEntryId);
            /* NP_NONNULL_PARAM_VIOLATION: https://github.com/findbugsproject/findbugs/issues/79 */
            return CompletableFuture.completedFuture(null);
        } catch (ObjectManagerException err) {
            CompletableFuture<Void> res = new CompletableFuture<>();
            res.completeExceptionally(err);
            return res;
        }
    }

    @Override
    public void start() {

    }

    @Override
    public void gc(String bucketId) {
        try {
            getBucket(bucketId).gc();
        } catch (ObjectManagerException ex) {
        }
    }

    @Override
    public void gc() {
        buckets.values().forEach(MemBucket::gc);
    }
}
