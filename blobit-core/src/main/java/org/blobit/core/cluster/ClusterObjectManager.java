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
package org.blobit.core.cluster;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.sql.DataSource;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.blobit.core.api.BucketConfiguration;
import org.blobit.core.api.BucketHandle;
import org.blobit.core.api.BucketMetadata;
import org.blobit.core.api.Configuration;
import org.blobit.core.api.DeletePromise;
import org.blobit.core.api.DownloadPromise;
import org.blobit.core.api.GetPromise;
import org.blobit.core.api.LocationInfo;
import org.blobit.core.api.NamedObjectConsumer;
import org.blobit.core.api.NamedObjectDeletePromise;
import org.blobit.core.api.NamedObjectDownloadPromise;
import org.blobit.core.api.NamedObjectFilter;
import org.blobit.core.api.NamedObjectGetPromise;
import org.blobit.core.api.NamedObjectMetadata;
import org.blobit.core.api.ObjectManager;
import org.blobit.core.api.ObjectManagerException;
import org.blobit.core.api.ObjectMetadata;
import org.blobit.core.api.ObjectNotFoundException;
import org.blobit.core.api.PutOptions;
import org.blobit.core.api.PutPromise;

/**
 * ObjectManager that uses Bookkeeper and HerdDB as clusterable backend
 *
 * @author diego.salvi
 */
public class ClusterObjectManager implements ObjectManager {

    private static final Logger LOG = Logger.getLogger(
            ClusterObjectManager.class.getName());
    private static final Consumer<Long> NULL_LEN_CALLBACK = (l) -> {
    };

    private final BookKeeperBlobManager blobManager;
    private final HerdDBMetadataStorageManager metadataManager;
    private final long ledgerMinTtl;

    public ClusterObjectManager(Configuration configuration,
            DataSource datasource) throws ObjectManagerException {
        super();
        this.ledgerMinTtl = configuration.getEmptyLedgerMinTtl();

        metadataManager = new HerdDBMetadataStorageManager(datasource,
                configuration);
        metadataManager.init();

        blobManager = new BookKeeperBlobManager(configuration, metadataManager);
    }

    private class BucketHandleImpl implements BucketHandle {

        private final String bucketId;

        public BucketHandleImpl(String bucketId) {
            this.bucketId = bucketId;
        }

        @Override
        public void gc() {
            try {
                gcBucket(bucketId);
            } catch (ObjectManagerException err) {
                LOG.log(Level.SEVERE, "error while cleaning " + bucketId, err);
            }
        }

        @Override
        public PutPromise put(String name, long length, InputStream input, PutOptions putOptions) {
            return blobManager.put(bucketId, name, length, input, putOptions);
        }

        @Override
        public PutPromise put(String name, byte[] data, int offset, int len, PutOptions putOptions) {
            return blobManager.put(bucketId, name, data, offset, len, putOptions);
        }

        @Override
        public void concat(String sourceName, String destName) throws ObjectManagerException {
            metadataManager.concat(bucketId, sourceName, destName);
        }

        @Override
        public void listByName(NamedObjectFilter filter, NamedObjectConsumer consumer) throws ObjectManagerException {
            metadataManager.listByName(bucketId, filter, consumer);
        }

        @Override
        public NamedObjectGetPromise getByName(String name) {
            try {
                List<String> ids = metadataManager.lookupObjectByName(bucketId,
                        name);
                if (ids.isEmpty()) {
                    CompletableFuture<List<byte[]>> res =
                            new CompletableFuture<>();
                    res.completeExceptionally(ObjectNotFoundException.INSTANCE);
                    return new NamedObjectGetPromise(Collections.emptyList(), 0,
                            res);
                }
                long size = 0;
                AtomicInteger remaining = new AtomicInteger(ids.size());
                CompletableFuture<List<byte[]>> result =
                        new CompletableFuture<>();
                // we are eagerly pre-sizing the array
                // it will be written from different threads
                final byte[][] data = new byte[(ids.size())][];
                int i = 0;
                for (String id : ids) {
                    final int j = i++;
                    GetPromise promise = get(id);
                    size += promise.length;
                    promise.future.whenComplete(
                            (byte[] part, Throwable err) -> {
                                LOG.log(Level.INFO,
                                        "get finished for part " + j + " remaining:" + remaining + " err: " + err,
                                        err);
                                if (err != null) {
                                    // fail fast
                                    result.completeExceptionally(err);
                                } else {
                                    data[j] = part;
                                    if (remaining.decrementAndGet() == 0) {
                                        LOG.log(Level.INFO, "completed !!");
                                        result.complete(Arrays.asList(data));
                                    } else {
                                        LOG.log(Level.INFO,
                                                "not yet completed, remaining is now " + remaining);
                                    }
                                }
                            });
                }
                return new NamedObjectGetPromise(ids, size, result);
            } catch (ObjectManagerException err) {
                return new NamedObjectGetPromise(Collections.emptyList(),
                        0, BookKeeperBlobManager.wrapGenericException(err));
            }
        }

        @Override
        public GetPromise get(String objectId) {
            return blobManager.get(bucketId, objectId);
        }

        @Override
        public NamedObjectMetadata statByName(String name) throws ObjectManagerException {
            List<String> objectIds = metadataManager.
                    lookupObjectByName(bucketId, name);
            if (objectIds.isEmpty()) {
                return null;
            }
            List<ObjectMetadata> objects = new ArrayList<>();
            long size = 0;
            for (String id : objectIds) {
                ObjectMetadata objectMetadata = BookKeeperBlobManager.stat(bucketId, id);
                if (objectMetadata == null) {
                    throw new ObjectNotFoundException(
                            "Object " + id + " was not found while"
                            + " reading named object '" + name + "'");
                }
                objects.add(objectMetadata);
                size += objectMetadata.size;
            }
            return new NamedObjectMetadata(name,
                    size,
                    objects);
        }

        @Override
        public ObjectMetadata stat(String objectId) {
            return BookKeeperBlobManager.stat(bucketId, objectId);
        }

        @Override
        public DownloadPromise download(String objectId,
                Consumer<Long> lengthCallback,
                OutputStream output, long offset,
                long length) {
            return blobManager.download(bucketId, objectId, lengthCallback,
                    output, offset, length);
        }

        @Override
        public NamedObjectDownloadPromise downloadByName(String name,
                Consumer<Long> lengthCallback,
                OutputStream output,
                int offset, long length) {
            List<String> ids = null;
            try {
                ids = metadataManager.lookupObjectByName(bucketId, name);
                if (ids == null || ids.isEmpty()) {
                    CompletableFuture<byte[]> res = new CompletableFuture<>();
                    res.completeExceptionally(ObjectNotFoundException.INSTANCE);
                    return new NamedObjectDownloadPromise(name, null, 0, res);
                }
                long totalLen = 0;
                List<BKEntryId> segments = new ArrayList<>();
                for (String id : ids) {
                    BKEntryId segment = BKEntryId.parseId(id);
                    totalLen += segment.length;
                    segments.add(segment);
                }
                long availableLength = totalLen;
                if (offset > 0) {
                    availableLength -= offset;
                }
                if (length < 0 || length > availableLength) { // full object
                    length = availableLength;
                }
                lengthCallback.accept(length);
                CompletableFuture<?> result = new CompletableFuture<>();
                NamedObjectDownloadPromise res = new NamedObjectDownloadPromise(
                        name, ids, length, result);
                if (length <= 0) {
                    // early exit, nothing to to
                    FutureUtils.complete(result, null);
                    return res;
                }

                int initialPart = 0;
                long offsetInStartingSegment = offset;

                if (offset > 0) {
                    // need to jump to the offset
                    // skipping first N parts
                    BKEntryId segment = segments.get(initialPart);
                    while (initialPart < segments.size()) {
                        long segmentLen = segment.length;
                        if (offsetInStartingSegment < segmentLen) {
                            // we have found the good segment to start from
                            break;
                        } else {
                            offsetInStartingSegment -= segmentLen;
                            initialPart++;
                        }
                    }
                    if (initialPart == segments.size()) {
                        throw new IllegalStateException();
                    }
                }
                startDownloadSegment(segments,
                        initialPart,
                        offsetInStartingSegment, length,
                        output, result);
                return res;
            } catch (ObjectManagerException err) {
                return new NamedObjectDownloadPromise(name, ids,
                        -1, BookKeeperBlobManager.wrapGenericException(err));
            }
        }

        private void startDownloadSegment(List<BKEntryId> segments, int index,
                long offsetInSegment,
                long remainingLen,
                OutputStream output,
                CompletableFuture<?> result) {
            BKEntryId currentSegment = segments.get(index);

            long lengthForCurrentSegment = Math.min(remainingLen,
                    currentSegment.length - offsetInSegment);

            DownloadPromise download = download(currentSegment.toId(),
                    NULL_LEN_CALLBACK,
                    output, offsetInSegment, lengthForCurrentSegment);
            download.future.whenComplete((a, error) -> {
                if (error != null) {

                    // fast fail, complete the future
                    result.completeExceptionally(error);
                } else {
                    long newRemainingLen =
                            remainingLen - lengthForCurrentSegment;

                    if (newRemainingLen == 0) {
                        FutureUtils.complete(result, null);
                    } else {
                        // start next segment
                        startDownloadSegment(segments,
                                index + 1, 0 /* offset */,
                                newRemainingLen,
                                output, result);
                    }
                }
            });
        }

        @Override
        @SuppressFBWarnings("NP_NONNULL_PARAM_VIOLATION")
        public NamedObjectDeletePromise deleteByName(String name) {
            try {
                List<String> ids = metadataManager.lookupObjectByName(bucketId,
                        name);
                CompletableFuture<?> res = new CompletableFuture<>();

                if (!ids.isEmpty()) {
                    AtomicInteger count = new AtomicInteger(ids.size());
                    for (String id : ids) {
                        DeletePromise delete = delete(id, name);
                        delete.future.whenComplete((x, error) -> {
                            if (error != null) {
                                res.completeExceptionally(error);
                            } else {
                                if (count.decrementAndGet() == 0) {
                                    FutureUtils.complete(res, null);
                                }
                            }

                        });
                    }
                } else {
                    res.completeExceptionally(ObjectNotFoundException.INSTANCE);
                }
                return new NamedObjectDeletePromise(name, ids, res);
            } catch (ObjectManagerException err) {
                return new NamedObjectDeletePromise(null, Collections.
                        emptyList(), BookKeeperBlobManager.wrapGenericException(
                                err));
            }
        }

        @Override
        public DeletePromise delete(String objectId) {
            return delete(objectId, null);
        }

        @SuppressFBWarnings("NP_NONNULL_PARAM_VIOLATION")
        private DeletePromise delete(String objectId, String name) {
            if (objectId == null) {
                return new DeletePromise(null, BookKeeperBlobManager.
                        wrapGenericException(new IllegalArgumentException(
                                "null id")));
            }
            CompletableFuture<Void> result = new CompletableFuture<>();
            if (BKEntryId.EMPTY_ENTRY_ID.equals(objectId)) {
                // nothing to delete!
                result.complete(null);

            } else {
                try {
                    BKEntryId bk = BKEntryId.parseId(objectId);
                    metadataManager.deleteObject(bucketId, bk.ledgerId,
                            bk.firstEntryId, name);
                    result.complete(null);
                } catch (ObjectManagerException ex) {
                    result.completeExceptionally(ex);
                }
            }
            return new DeletePromise(objectId, result);
        }

        @Override
        public CompletableFuture<? extends LocationInfo> getLocationInfo(
                String objectId) throws ObjectManagerException {
            BKEntryId bk = BKEntryId.parseId(objectId);
            return blobManager.getLocationInfo(bk);
        }

    }

    @Override
    public BucketHandle getBucket(String bucketId) {
        return new BucketHandleImpl(bucketId);
    }

    @Override
    public CompletableFuture<BucketMetadata> createBucket(String bucketId,
            String tablespaceName,
            BucketConfiguration configuration) {
        return metadataManager.createBucket(bucketId, tablespaceName,
                configuration);
    }

    @Override
    public void listBuckets(Consumer<BucketMetadata> consumer) throws ObjectManagerException {
        metadataManager.listBuckets(consumer);
    }

    @Override
    public void gc() {
        try {
            metadataManager.listBuckets((BucketMetadata bucket) -> {
                try {
                    String bucketId = bucket.getBucketId();
                    gcBucket(bucketId);
                } catch (ObjectManagerException ex) {
                    LOG.log(Level.SEVERE, "Error during gc of bucket " + bucket.
                            getBucketId(), ex);
                }
            });
        } catch (ObjectManagerException ex) {
            LOG.log(Level.SEVERE, "Error during ledger management", ex);
        }
    }

    private void gcBucket(String bucketId) throws ObjectManagerException {
        Collection<Long> ledgers = metadataManager.
                listDeletableLedgers(bucketId, ledgerMinTtl);
        LOG.log(Level.INFO, "There are {0} deletable ledgers for bucket {1}",
                new Object[]{ledgers.size(), bucketId});
        for (long idledger : ledgers) {
            boolean ok = blobManager.dropLedger(idledger);
            if (ok) {
                metadataManager.deleteLedger(bucketId, idledger);
            } else if (LOG.isLoggable(Level.FINE)) {
                // this is very common for a ledger that it is currently in use
                LOG.log(Level.FINE, "Drop ledger {0} failed", idledger);
            }
        }
    }

    @Override
    public void start() throws ObjectManagerException {
        /* NOP */
    }

    @Override
    public void close() {
        blobManager.close();
    }

    public BookKeeperBlobManager getBlobManager() {
        return blobManager;
    }

    public HerdDBMetadataStorageManager getMetadataManager() {
        return metadataManager;
    }

    @Override
    public CompletableFuture<?> deleteBucket(String bucketId) {
        return metadataManager.markBucketForDeletion(bucketId);
    }

    @Override
    public BucketMetadata getBucketMetadata(String bucketId) throws ObjectManagerException {
        return metadataManager.getBucketMetadata(bucketId);
    }

    @Override
    public void cleanup() throws ObjectManagerException {
        List<BucketMetadata> buckets = metadataManager.
                selectBucketsMarkedForDeletion();
        if (buckets.isEmpty()) {
            return;
        }

        // sort in order to re-play the work from when it started
        buckets.sort(Comparator.comparing(BucketMetadata::getUuid));

        // delete references to the bucket from bucket-wide metadata
        for (BucketMetadata bucket : buckets) {
            LOG.log(Level.INFO, "found {0} uuid {1} to be erased",
                    new Object[]{bucket.getBucketId(), bucket.getUuid()});
            metadataManager.cleanupDeletedBucketByUuid(bucket);
        }

        // delete data from BookKeeper
        blobManager.scanAndDeleteLedgersForBuckets(buckets);

        // delete references to the bucket from system wide metadata
        for (BucketMetadata bucket : buckets) {
            metadataManager.deletedBucketByUuid(bucket);
        }

    }

}
