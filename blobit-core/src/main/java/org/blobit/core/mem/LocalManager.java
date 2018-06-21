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
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import org.blobit.core.api.BucketConfiguration;
import org.blobit.core.api.BucketMetadata;
import org.blobit.core.api.LedgerMetadata;
import org.blobit.core.api.ObjectManager;
import org.blobit.core.api.ObjectManagerException;
import org.blobit.core.api.ObjectMetadata;
import org.blobit.core.api.PutPromise;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.UUID;
import java.util.function.Consumer;
import org.blobit.core.api.BucketHandle;
import org.blobit.core.api.DeletePromise;
import org.blobit.core.api.DownloadPromise;
import org.blobit.core.api.GetPromise;

/**
 * MetadataManager all in memory for unit tests
 *
 * @author enrico.olivelli
 */
public class LocalManager implements ObjectManager {

    private final Map<String, MemBucket> buckets = new ConcurrentHashMap<>();

    @Override
    public CompletableFuture<BucketMetadata> createBucket(String name, String bucketTableSpaceName, BucketConfiguration configuration) {

        CompletableFuture<BucketMetadata> res = new CompletableFuture<>();
        MemBucket oldBucket = buckets.computeIfAbsent(name, (bname) -> {
            return new MemBucket(bname, configuration);
        });
        if (oldBucket == null) {
            res.completeExceptionally(new ObjectManagerException("bucket " + name + " already exists").fillInStackTrace());
        } else {
            res.complete(new BucketMetadata(name, UUID.randomUUID().toString(), BucketMetadata.STATUS_ACTIVE,
                    configuration, bucketTableSpaceName));
        }
        return res;
    }

    @Override
    public void listBuckets(Consumer<BucketMetadata> consumer) throws ObjectManagerException {
        buckets
                .values()
                .stream()
                .map(MemBucket::getMetadata)
                .forEach(consumer);
    }

    @Override
    public BucketMetadata getBucketMetadata(String bucketId) throws ObjectManagerException {
        return getMemBucket(bucketId).getMetadata();
    }

    private class BucketHandleImpl implements BucketHandle {

        private final String bucketId;

        public BucketHandleImpl(String bucketId) {
            this.bucketId = bucketId;
        }

        @Override
        public PutPromise put(byte[] data) {
            return put(data, 0, data.length);
        }

        @Override
        public PutPromise put(long length, InputStream input) {
            DataInputStream ii = new DataInputStream(input);
            // we are in-memory, we can store only 'small' objects
            byte[] content = new byte[(int) length];
            try {
                ii.readFully(content);
            } catch (IOException err) {
                CompletableFuture<Void> res = new CompletableFuture<>();
                res.completeExceptionally(err);
                return new PutPromise(null, res);
            }
            return put(content);
        }

        @Override
        @SuppressFBWarnings("NP_NONNULL_PARAM_VIOLATION")
        public PutPromise put(byte[] data, int offset, int len) {
            try {
                if (offset != 0 && len < data.length) {
                    byte[] copy = new byte[len];
                    System.arraycopy(data, offset, copy, 0, len);
                    data = copy;
                }
                MemEntryId res = getMemBucket(bucketId).getCurrentLedger().put(data);
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
        public GetPromise get(String objectId) {
            try {
                MemEntryId id = MemEntryId.parseId(objectId);
                byte[] res = getMemBucket(bucketId).getLedger(id.ledgerId).get(id.firstEntryId);
                /* NP_NONNULL_PARAM_VIOLATION: https://github.com/findbugsproject/findbugs/issues/79 */
                return new GetPromise(objectId, res.length, CompletableFuture.completedFuture(res));
            } catch (ObjectManagerException err) {
                CompletableFuture<byte[]> res = new CompletableFuture<>();
                res.completeExceptionally(err);
                return new GetPromise(objectId, 0, res);
            }
        }

        @SuppressFBWarnings("NP_NONNULL_PARAM_VIOLATION")
        @Override
        public DownloadPromise download(String objectId, Consumer<Long> lengthCallback, OutputStream output, int offset, long length) {
            try {
                GetPromise result = get(objectId);
                lengthCallback.accept(result.length);
                byte[] data = result.get();
                ByteArrayInputStream ii = new ByteArrayInputStream(data);
                ii.skip(offset);
                int countWritten = 0;
                if (length < 0) {
                    int b = ii.read();
                    while (b != -1) {
                        output.write(b);
                        countWritten++;
                        b = ii.read();
                    }
                } else {
                    long remaining = length;
                    int b = ii.read();
                    while (remaining-- > 0 && b != -1) {
                        output.write(b);
                        countWritten++;
                        b = ii.read();
                    }
                }
                return new DownloadPromise(objectId, countWritten, CompletableFuture.completedFuture(null));
            } catch (IOException | InterruptedException | ObjectManagerException err) {
                CompletableFuture<Void> res = new CompletableFuture<>();
                res.completeExceptionally(err);
                return new DownloadPromise(objectId, 0, res);
            }
        }

        @SuppressFBWarnings("NP_NONNULL_PARAM_VIOLATION")
        @Override
        public DeletePromise delete(String objectId) {
            try {
                MemEntryId id = MemEntryId.parseId(objectId);
                getMemBucket(bucketId).getLedger(id.ledgerId).delete(id.firstEntryId);
                /* NP_NONNULL_PARAM_VIOLATION: https://github.com/findbugsproject/findbugs/issues/79 */
                return new DeletePromise(objectId, CompletableFuture.completedFuture(null));
            } catch (ObjectManagerException err) {
                CompletableFuture<Void> res = new CompletableFuture<>();
                res.completeExceptionally(err);
                return new DeletePromise(objectId, res);
            }
        }
    }

    @Override
    public BucketHandle getBucket(String bucketId) {
        return new BucketHandleImpl(bucketId);
    }

    private MemBucket getMemBucket(String bucketId) throws ObjectManagerException {
        MemBucket bucket = buckets.get(bucketId);
        if (bucket == null) {
            throw new ObjectManagerException("bucket " + bucketId + " does not exist");
        }
        return bucket;
    }

    @Override
    public CompletableFuture<?> deleteBucket(String bucketId) {
        buckets.remove(bucketId);
        return CompletableFuture.completedFuture(null);
    }

    Collection<Long> listDeletableLedgers(String bucketId) throws ObjectManagerException {
        return getMemBucket(bucketId).listDeletableLedgers();
    }

    Collection<LedgerMetadata> listLedgersbyBucketId(String bucketId) throws ObjectManagerException {
        return getMemBucket(bucketId).listLedgers();
    }

    Collection<ObjectMetadata> listObjectsByLedger(String bucketId, long ledgerId) throws ObjectManagerException {
        return getMemBucket(bucketId).getLedger(ledgerId).listObjects();
    }

    @Override
    public void close() {
        buckets.clear();
    }

    @Override
    public void cleanup() throws ObjectManagerException {
    }

    @Override
    public void start() {

    }

    @Override
    public void gc(String bucketId) {
        try {
            getMemBucket(bucketId).gc();
        } catch (ObjectManagerException ex) {
        }
    }

    @Override
    public void gc() {
        buckets.values().forEach(MemBucket::gc);
    }
}
