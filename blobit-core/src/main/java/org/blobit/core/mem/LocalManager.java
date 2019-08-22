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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.blobit.core.api.BucketConfiguration;
import org.blobit.core.api.BucketHandle;
import org.blobit.core.api.BucketMetadata;
import org.blobit.core.api.DeletePromise;
import org.blobit.core.api.DownloadPromise;
import org.blobit.core.api.GetPromise;
import org.blobit.core.api.LedgerMetadata;
import org.blobit.core.api.LocationInfo;
import org.blobit.core.api.LocationInfo.ServerInfo;
import org.blobit.core.api.NamedObjectConsumer;
import org.blobit.core.api.NamedObjectDeletePromise;
import org.blobit.core.api.NamedObjectDownloadPromise;
import org.blobit.core.api.NamedObjectFilter;
import org.blobit.core.api.NamedObjectGetPromise;
import org.blobit.core.api.NamedObjectMetadata;
import org.blobit.core.api.ObjectAlreadyExistsException;
import org.blobit.core.api.ObjectManager;
import org.blobit.core.api.ObjectManagerException;
import org.blobit.core.api.ObjectMetadata;
import org.blobit.core.api.ObjectNotFoundException;
import org.blobit.core.api.PutOptions;
import org.blobit.core.api.PutPromise;
import org.blobit.core.filters.NamePrefixFilter;

/**
 * MetadataManager all in memory for unit tests
 *
 * @author enrico.olivelli
 */
public class LocalManager implements ObjectManager {

    private static final Consumer<Long> NULL_LEN_CALLBACK = (l) -> {
    };
    private static final List<ServerInfo> LOCAL_SERVER =
            Collections.unmodifiableList(Arrays.asList(
                    (ServerInfo) () -> "local-vm"));
    private static final List<Long> OFFSET_0 =
            Collections.unmodifiableList(Arrays.asList(0L));

    private static <T> CompletableFuture<T> wrapGenericException(Exception err) {
        CompletableFuture<T> error = new CompletableFuture<>();
        error.completeExceptionally(new ObjectManagerException(err));
        return error;
    }
    private final Map<String, MemBucket> buckets = new ConcurrentHashMap<>();

    @Override
    public CompletableFuture<BucketMetadata> createBucket(String name,
                                                          String bucketTableSpaceName,
                                                          BucketConfiguration configuration) {

        CompletableFuture<BucketMetadata> res = new CompletableFuture<>();
        MemBucket oldBucket = buckets.computeIfAbsent(name, (bname) -> {
            return new MemBucket(bname, configuration);
        });
        if (oldBucket == null) {
            res.completeExceptionally(new ObjectManagerException(
                    "bucket " + name + " already exists").fillInStackTrace());
        } else {
            res.complete(new BucketMetadata(name, UUID.randomUUID().toString(),
                    BucketMetadata.STATUS_ACTIVE,
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

    @Override

    public BucketHandle getBucket(String bucketId) {
        return new BucketHandleImpl(bucketId);
    }

    private MemBucket getMemBucket(String bucketId) throws ObjectManagerException {
        MemBucket bucket = buckets.get(bucketId);
        if (bucket == null) {
            throw new ObjectManagerException(
                    "bucket " + bucketId + " does not exist");
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

    Collection<ObjectMetadata> listObjectsByLedger(String bucketId,
                                                   long ledgerId) throws ObjectManagerException {
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
    public void gc() {
        buckets.values().forEach(MemBucket::gc);
    }

    private static final class MemLocationInfo implements LocationInfo {

        private final MemEntryId id;

        public MemLocationInfo(MemEntryId id) {
            this.id = id;
        }

        @Override
        public String getId() {
            return id.toId();
        }

        @Override
        public List<ServerInfo> getServersAtPosition(long offset) {
            if (offset < 0 || offset >= id.length) {
                return Collections.emptyList();
            }
            return LOCAL_SERVER;
        }

        @Override
        public long getSize() {
            return id.length;
        }

        @Override
        public List<Long> getSegmentsStartOffsets() {
            if (id.length == 0) {
                return Collections.emptyList();
            }
            return OFFSET_0;
        }

    }

    private class BucketHandleImpl implements BucketHandle {

        private final String bucketId;
        private final ConcurrentHashMap<String, List<String>> objectNames =
                new ConcurrentHashMap<>();

        public BucketHandleImpl(String bucketId) {
            this.bucketId = bucketId;
        }

        @Override
        public void concat(String sourceName, String destName) throws ObjectManagerException {
            List<String> prevList = objectNames.get(sourceName);
            if (prevList == null) {
                throw new ObjectNotFoundException(sourceName);
            }
            objectNames.compute(destName, (n, currentList) -> {
                if (currentList == null) {
                    return new ArrayList<>(prevList);
                } else {
                    List<String> newList = new ArrayList<>(currentList);
                    newList.addAll(prevList);
                    return newList;
                }
            });
            objectNames.remove(sourceName);
        }

        @Override
        public PutPromise put(String name, long length, InputStream input, PutOptions putOptions) {
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
            return put(name, content, 0, content.length, putOptions);
        }

        @Override
        @SuppressFBWarnings("NP_NONNULL_PARAM_VIOLATION")
        public PutPromise put(String name, byte[] data, int offset, int len, PutOptions putOptions) {
            try {
                if (offset != 0 && len < data.length) {
                    byte[] copy = new byte[len];
                    System.arraycopy(data, offset, copy, 0, len);
                    data = copy;
                }
                MemEntryId res = getMemBucket(bucketId).getCurrentLedger().put(
                        data);
                /* NP_NONNULL_PARAM_VIOLATION: https://github.com/findbugsproject/findbugs/issues/79 */
                if (name != null) {
                    if (putOptions.isOverwrite()) {
                        List<String> prevList = objectNames.put(name, Arrays.asList(res.toId()));
                        if (prevList != null) {
                            // drop old unreferenced objects
                            for (String objectId : prevList) {
                                delete(objectId);
                            }
                        }
                    } else if (putOptions.isAppend()) {
                        objectNames.compute(name, (n, prevList) -> {
                            if (prevList == null) {
                                return Arrays.asList(res.toId());
                            } else {
                                // prepend prevList to the new list
                                List<String> resList = new ArrayList<>(prevList);
                                resList.add(res.toId());
                                return resList;
                            }
                        });
                    } else {
                        List<String> list = Arrays.asList(res.toId());
                        List<String> result = objectNames.computeIfAbsent(name, (n) -> list);
                        if (list != result) {
                            return new PutPromise(null, FutureUtils.exception(new ObjectAlreadyExistsException(name)));
                        }
                    }
                }
                return new PutPromise(res.toId(), CompletableFuture.
                        <Void>completedFuture(null));
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
                byte[] res = getMemBucket(bucketId).getLedger(id.ledgerId).get(
                        id.entryId);
                /* NP_NONNULL_PARAM_VIOLATION: https://github.com/findbugsproject/findbugs/issues/79 */
                return new GetPromise(objectId, res.length, CompletableFuture.
                        completedFuture(res));
            } catch (ObjectManagerException err) {
                CompletableFuture<byte[]> res = new CompletableFuture<>();
                res.completeExceptionally(err);
                return new GetPromise(null, 0, res);
            }
        }

        @Override
        public NamedObjectGetPromise getByName(String name) {
            List<String> ids = objectNames.get(name);
            if (ids == null || ids.isEmpty()) {
                CompletableFuture<List<byte[]>> res = new CompletableFuture<>();
                res.completeExceptionally(ObjectNotFoundException.INSTANCE);
                return new NamedObjectGetPromise(null, 0, res);
            }

            long size = 0;
            AtomicInteger remaining = new AtomicInteger(ids.size());
            CompletableFuture<List<byte[]>> result = new CompletableFuture<>();
            // we are pre-allocating the array
            // so the list won't grow and we can write
            // from multiple threads
            final byte[][] data = new byte[(ids.size())][];
            int i = 0;
            for (String id : ids) {
                final int _i = i++;
                GetPromise promise = get(id);
                size += promise.length;
                promise.future.whenComplete((byte[] part, Throwable err) -> {
                    if (err != null) {
                        result.completeExceptionally(err);
                    } else {
                        data[_i] = part;
                        if (remaining.decrementAndGet() == 0) {
                            result.complete(Arrays.asList(data));
                        }
                    }
                });
            }
            return new NamedObjectGetPromise(ids, size, result);
        }

        @Override
        public NamedObjectMetadata statByName(String name) {
            List<String> ids = objectNames.get(name);
            if (ids == null) {
                return null;
            } else {
                List<ObjectMetadata> parts = new ArrayList<>();
                long size = 0;
                for (String id : ids) {
                    GetPromise get = this.get(id);
                    size += get.length;
                    parts.add(new ObjectMetadata(id, get.length));
                }
                return new NamedObjectMetadata(name,
                        size, parts);
            }
        }

        @Override
        public ObjectMetadata stat(String objectId) {
            GetPromise get = get(objectId);
            if (get.id == null) {
                return null;
            } else {
                return new ObjectMetadata(objectId, get.length);
            }
        }

        @Override
        public NamedObjectDownloadPromise downloadByName(String name,
                                                         Consumer<Long> lengthCallback,
                                                         OutputStream output,
                                                         int offset, long length) {
            List<String> ids = null;
            try {
                ids = objectNames.get(name);
                if (ids == null || ids.isEmpty()) {
                    CompletableFuture<byte[]> res = new CompletableFuture<>();
                    res.completeExceptionally(ObjectNotFoundException.INSTANCE);
                    return new NamedObjectDownloadPromise(name, null, 0, res);
                }
                long totalLen = 0;
                List<MemEntryId> segments = new ArrayList<>();
                for (String id : ids) {
                    MemEntryId segment = MemEntryId.parseId(id);
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
                    MemEntryId segment = segments.get(initialPart);
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
                        -1, wrapGenericException(err));
            }
        }

        private void startDownloadSegment(List<MemEntryId> segments, int index,
                                          long offsetInSegment,
                                          long remainingLen,
                                          OutputStream output,
                                          CompletableFuture<?> result) {
            MemEntryId currentSegment = segments.get(index);

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
            CompletableFuture<?> res = new CompletableFuture<>();
            List<String> ids = objectNames.remove(name);
            if (ids != null && !ids.isEmpty()) {
                AtomicInteger count = new AtomicInteger(ids.size());
                for (String id : ids) {
                    DeletePromise delete = delete(id);
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
                // empty object ? nothing to delete
                FutureUtils.complete(res, null);
            }
            return new NamedObjectDeletePromise(name, ids, res);

        }

        @SuppressFBWarnings("NP_NONNULL_PARAM_VIOLATION")
        @Override
        public DownloadPromise download(String objectId,
                                        Consumer<Long> lengthCallback,
                                        OutputStream output,
                                        long offset, long length) {
            try {
                GetPromise result = get(objectId);
                final long realLen;
                if (length < 0) {
                    realLen = result.length - offset;
                } else if (length > result.length) {
                    realLen = length;
                } else {
                    realLen = result.length;
                }
                lengthCallback.accept(realLen);
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
                return new DownloadPromise(objectId, countWritten,
                        CompletableFuture.completedFuture(null));
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
                getMemBucket(bucketId).getLedger(id.ledgerId).delete(id.entryId);
                /* NP_NONNULL_PARAM_VIOLATION: https://github.com/findbugsproject/findbugs/issues/79 */
                return new DeletePromise(objectId, CompletableFuture.
                        completedFuture(null));
            } catch (ObjectManagerException err) {
                CompletableFuture<Void> res = new CompletableFuture<>();
                res.completeExceptionally(err);
                return new DeletePromise(objectId, res);
            }
        }

        @Override
        public void gc() {
            try {
                getMemBucket(bucketId).gc();
            } catch (ObjectManagerException ex) {
            }
        }

        @Override
        public CompletableFuture<? extends LocationInfo> getLocationInfo(
                String objectId) {
            CompletableFuture<MemLocationInfo> result =
                    new CompletableFuture<>();
            try {
                MemEntryId id = MemEntryId.parseId(objectId);
                result.complete(new MemLocationInfo(id));
            } catch (ObjectManagerException err) {
                result.completeExceptionally(err);
            }
            return result;
        }

        @Override
        public void listByName(NamedObjectFilter filter, NamedObjectConsumer consumer) throws ObjectManagerException {
            if (filter instanceof NamePrefixFilter) {
                NamePrefixFilter pFilter = (NamePrefixFilter) filter;
                String prefix = pFilter.getPrefix();
                for (String name : objectNames.keySet()) {
                    if (!name.startsWith(prefix)) {
                        continue;
                    }
                    NamedObjectMetadata metadata = statByName(name);
                    if (metadata != null) {
                        boolean goOn = consumer.accept(metadata);
                        if (!goOn) {
                            break;
                        }
                    }
                }
                return;
            }
            throw new ObjectManagerException("Unsupported filter type " + filter);
        }

    }

}
