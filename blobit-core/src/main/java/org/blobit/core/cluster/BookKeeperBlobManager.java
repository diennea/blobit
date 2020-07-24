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

import static org.blobit.core.cluster.BucketWriter.BK_METADATA_BUCKET_ID;
import static org.blobit.core.cluster.BucketWriter.BK_METADATA_BUCKET_UUID;
import static org.blobit.core.cluster.BucketWriter.DUMMY_PWD;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.meta.HierarchicalLedgerManagerFactory;
import org.apache.commons.pool2.KeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.blobit.core.api.BucketMetadata;
import org.blobit.core.api.Configuration;
import org.blobit.core.api.DownloadPromise;
import org.blobit.core.api.GetPromise;
import org.blobit.core.api.LocationInfo;
import org.blobit.core.api.ObjectManagerException;
import org.blobit.core.api.ObjectMetadata;
import org.blobit.core.api.PutOptions;
import org.blobit.core.api.PutPromise;

/**
 * Stores Objects on Apache BookKeeper
 *
 * @author enrico.olivelli
 */
public class BookKeeperBlobManager implements AutoCloseable {

    private static final Logger LOG = Logger.getLogger(
            BookKeeperBlobManager.class.getName());

    private final HerdDBMetadataStorageManager metadataStorageManager;
    private final BookKeeper bookKeeper;
    final GenericKeyedObjectPool<String, BucketWriter> writers;
    final GenericKeyedObjectPool<Long, BucketReader> readers;
    private final int replicationFactor;
    private final long maxBytesPerLedger;
    private final int maxEntrySize;
    private final boolean enableChecksum;
    private final boolean deferredSync;
    private final ExecutorService callbacksExecutor;
    private final ExecutorService threadpool = Executors.
            newSingleThreadExecutor();
    private ConcurrentMap<Long, BucketWriter> activeWriters =
            new ConcurrentHashMap<>();
    private final Stats stats = new Stats();
    private final long maxWriterTtl;

    CompletableFuture<? extends LocationInfo> getLocationInfo(BKEntryId bk) {
        CompletableFuture<BKLocationInfo> result = new CompletableFuture<>();
        bookKeeper.getLedgerManager()
                .readLedgerMetadata(bk.ledgerId)
                .whenComplete((versionedLedgerMetadata, error) -> {
                    if (error != null) {
                        result.completeExceptionally(new ObjectManagerException(
                                error));
                    } else {
                        result.complete(new BKLocationInfo(bk,
                                versionedLedgerMetadata.getValue()));
                    }

                });
        return result;
    }

    public static final class Stats {

        private final LongAdder usedWritersAsReaders = new LongAdder();

        public long getUsedWritersAsReaders() {
            return usedWritersAsReaders.longValue();
        }
    }

    public PutPromise put(String bucketId, String name, long len, InputStream in, PutOptions putOptions) {
        if (len == 0) {
            // very special case, the empty blob
            CompletableFuture<Void> result = new CompletableFuture<>();
            result.complete(null);
            return new PutPromise(BKEntryId.EMPTY_ENTRY_ID, result);
        }
        try {
            BucketWriter writer = writers.borrowObject(bucketId);
            try {
                return writer
                        .writeBlob(bucketId, name, len, in, putOptions);
            } finally {
                writers.returnObject(bucketId, writer);
            }
        } catch (Exception err) {
            return new PutPromise(null, wrapGenericException(err));
        }
    }

    public PutPromise put(String bucketId, String name, byte[] data, int offset,
            int len, PutOptions putOptions) {
        if (data.length < offset + len || offset < 0 || len < 0) {
            throw new IndexOutOfBoundsException();
        }
        if (len == 0) {
            CompletableFuture<Void> result = new CompletableFuture<>();
            if (name == null) {
                // very special case, the unnamed empty blob
                result.complete(null);
            } else {
                try {
                    metadataStorageManager.appendEmptyObject(bucketId,
                            name, putOptions.isOverwrite());
                    result.complete(null);
                } catch (ObjectManagerException err) {
                    result.completeExceptionally(err);
                }
            }
            return new PutPromise(BKEntryId.EMPTY_ENTRY_ID, result);
        }
        try {
            BucketWriter writer = writers.borrowObject(bucketId);
            try {
                return writer
                        .writeBlob(bucketId, name, data, offset, len, putOptions);
            } finally {
                writers.returnObject(bucketId, writer);
            }
        } catch (Exception err) {
            return new PutPromise(null, wrapGenericException(err));
        }
    }

    static <T> CompletableFuture<T> wrapGenericException(Exception err) {
        CompletableFuture<T> error = new CompletableFuture<>();
        error.completeExceptionally(new ObjectManagerException(err));
        return error;
    }

    static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

    DownloadPromise download(String bucketId, String id,
            Consumer<Long> lengthCallback, OutputStream output,
            long offset,
            long length) {
        if (id == null) {
            return new DownloadPromise(null, 0, wrapGenericException(
                    new IllegalArgumentException("null id")));
        }
        if (BKEntryId.EMPTY_ENTRY_ID.equals(id) || length == 0) {
            lengthCallback.accept(0L);
            CompletableFuture<byte[]> result = new CompletableFuture<>();
            result.complete(null);
            return new DownloadPromise(id, 0, result);
        }
        try {
            BKEntryId entry = BKEntryId.parseId(id);
            if (offset >= entry.length) {
                lengthCallback.accept(0L);
                CompletableFuture<byte[]> result = new CompletableFuture<>();
                result.complete(null);
                return new DownloadPromise(id, 0, result);
            }
            // notify the called about the expected length (for instance an HTTP server will send the
            // Content-Length header)
            long finalLength;
            if (length < 0) {
                finalLength = entry.length;
            } else {
                finalLength = Math.min(length, entry.length);
            }
            if (finalLength < 0) {
                finalLength = 0;
            }
            if (finalLength > entry.length - offset) {
                lengthCallback.accept(entry.length - offset);
            } else {
                lengthCallback.accept(finalLength);
            }

            BucketReader reader = readers.borrowObject(entry.ledgerId);
            try {
                CompletableFuture<?> result = reader
                        .streamObject(entry.firstEntryId,
                                entry.firstEntryId + entry.numEntries - 1,
                                finalLength, entry.entrySize, entry.length,
                                output, offset);
                return new DownloadPromise(id, entry.length, result);
            } finally {
                readers.returnObject(entry.ledgerId, reader);
            }
        } catch (Exception err) {
            return new DownloadPromise(id, 0, wrapGenericException(err));
        }
    }

    GetPromise get(String bucketId, String id) {
        if (id == null) {
            return new GetPromise(null, 0, wrapGenericException(
                    new IllegalArgumentException("null id")));
        }
        if (BKEntryId.EMPTY_ENTRY_ID.equals(id)) {
            CompletableFuture<byte[]> result = new CompletableFuture<>();
            result.complete(EMPTY_BYTE_ARRAY);
            return new GetPromise(id, 0, result);
        }
        try {
            BKEntryId entry = BKEntryId.parseId(id);
            // as we are returing a byte[] we are limited to an int length
            if (entry.length >= Integer.MAX_VALUE) {
                return new GetPromise(id, 0, wrapGenericException(
                        new UnsupportedOperationException(
                                "Cannot read an " + entry.length + " bytes object into a byte[]")));
            }
            final int resultSize = (int) entry.length;
            BucketReader reader = readers.borrowObject(entry.ledgerId);
            try {
                CompletableFuture<byte[]> result = reader
                        .readObject(entry.firstEntryId,
                                entry.firstEntryId + entry.numEntries - 1,
                                resultSize);
                return new GetPromise(id, entry.length, result);
            } finally {
                readers.returnObject(entry.ledgerId, reader);
            }
        } catch (Exception err) {
            return new GetPromise(id, 0, wrapGenericException(err));
        }
    }

    static ObjectMetadata stat(String bucketId, String id) {
        if (id == null) {
            return null;
        }
        if (BKEntryId.EMPTY_ENTRY_ID.equals(id)) {
            return new ObjectMetadata(id, 0);
        }

        BKEntryId entry = BKEntryId.parseId(id);
        return new ObjectMetadata(id, entry.length);
    }

    private final class WritersFactory implements
            KeyedPooledObjectFactory<String, BucketWriter> {

        @Override
        public PooledObject<BucketWriter> makeObject(String bucketId) throws Exception {
            BucketWriter writer = new BucketWriter(bucketId,
                    bookKeeper, replicationFactor, maxEntrySize,
                    maxBytesPerLedger,
                    enableChecksum,
                    deferredSync, metadataStorageManager,
                    BookKeeperBlobManager.this,
                    System.currentTimeMillis() + maxWriterTtl
            );
            activeWriters.put(writer.getId(), writer);
            DefaultPooledObject<BucketWriter> be = new DefaultPooledObject<>(
                    writer);
            return be;
        }

        @Override
        public void destroyObject(String k, PooledObject<BucketWriter> po) throws Exception {
            po.getObject().close();
        }

        @Override
        public boolean validateObject(String k, PooledObject<BucketWriter> po) {
            return po.getObject().isValid();
        }

        @Override
        public void activateObject(String k, PooledObject<BucketWriter> po) throws Exception {
        }

        @Override
        public void passivateObject(String k, PooledObject<BucketWriter> po) throws Exception {
        }
    }

    private final class ReadersFactory implements
            KeyedPooledObjectFactory<Long, BucketReader> {

        @Override
        public PooledObject<BucketReader> makeObject(Long ledgerId) throws Exception {
            BucketReader reader;
            BucketWriter writer = activeWriters.get(ledgerId);
            if (writer != null && writer.isValid()) {
                stats.usedWritersAsReaders.increment();
                // the reader will se the LedgerHandle internal to the BucketWriter
                // as a 'reader' the LedgerHandle will continue to work even if 'closed'
                // because in BookKeeper 'closed' means something like 'sealed'
                reader = new BucketReader(writer.getLh(),
                        BookKeeperBlobManager.this);
            } else {
                reader = new BucketReader(ledgerId, bookKeeper,
                        BookKeeperBlobManager.this);
            }

            return new DefaultPooledObject<>(reader);
        }

        @Override
        public void destroyObject(Long ledgerId, PooledObject<BucketReader> po) throws Exception {
            po.getObject().close();

        }

        @Override
        public boolean validateObject(Long k, PooledObject<BucketReader> po) {
            return po.getObject().isValid();
        }

        @Override
        public void activateObject(Long k, PooledObject<BucketReader> po) throws Exception {

        }

        @Override
        public void passivateObject(Long k, PooledObject<BucketReader> po) throws Exception {

        }
    }

    public BookKeeperBlobManager(Configuration configuration,
            HerdDBMetadataStorageManager metadataStorageManager) throws ObjectManagerException {
        try {
            this.replicationFactor = configuration.getReplicationFactor();
            this.maxBytesPerLedger = configuration.getMaxBytesPerLedger();
            this.maxEntrySize = configuration.getMaxEntrySize();
            this.metadataStorageManager = metadataStorageManager;
            int concurrentWrites = configuration.getConcurrentWriters();
            int concurrentReaders = configuration.getMaxReaders();
            this.enableChecksum = configuration.isEnableChecksum();
            this.deferredSync = configuration.isDeferredSync();

            this.callbacksExecutor = Executors.newFixedThreadPool(
                    concurrentWrites);
            ClientConfiguration clientConfiguration = new ClientConfiguration();
            clientConfiguration.setThrottleValue(0);
            clientConfiguration.setLedgerManagerFactoryClass(
                    HierarchicalLedgerManagerFactory.class);
            clientConfiguration.setEnableDigestTypeAutodetection(true);

            String zkLedgersRootPath = configuration.getProperty(
                    Configuration.BOOKKEEPER_ZK_LEDGERS_ROOT_PATH,
                    Configuration.BOOKKEEPER_ZK_LEDGERS_ROOT_PATH_DEFAULT);
            String zkServers = configuration.getZookkeeperUrl();
            String metadataServiceURI = "zk+null://" + zkServers.replace(",",
                    ";") + zkLedgersRootPath;
            LOG.log(Level.INFO,
                    "BlobIt client is using BookKeeper metadataservice URI: {0}",
                    metadataServiceURI);
            clientConfiguration.setMetadataServiceUri(metadataServiceURI);
            clientConfiguration.setZkTimeout(configuration.getZookkeeperTimeout());
            for (String key : configuration.keys()) {
                if (key.startsWith("bookkeeper.")) {
                    String rawKey = key.substring("bookkeeper.".length());
                    clientConfiguration.setProperty(rawKey, configuration.
                            getProperty(key));
                }
            }
            LOG.info(
                    "ObjectManager will use BookKeeper ensemble at " + configuration.
                            getZookkeeperUrl() + " (" + configuration.getZookkeeperTimeout() + " ms timeout), that is BK configuration bookkeeper.metadataServiceUri="
                    + clientConfiguration.
                            getMetadataServiceUriUnchecked());

            GenericKeyedObjectPoolConfig configWriters =
                    new GenericKeyedObjectPoolConfig();
            configWriters.setMaxTotalPerKey(concurrentWrites);
            configWriters.setMaxIdlePerKey(concurrentWrites);
            configWriters.setTestOnReturn(true);
            configWriters.setTestOnBorrow(true);
            configWriters.setBlockWhenExhausted(true);
            this.writers = new GenericKeyedObjectPool<>(new WritersFactory(),
                    configWriters);

            GenericKeyedObjectPoolConfig configReaders =
                    new GenericKeyedObjectPoolConfig();
            configReaders.setMaxTotalPerKey(1);
            configReaders.setMaxIdlePerKey(1);
            configReaders.setMaxTotal(concurrentReaders);
            configReaders.setTestOnReturn(true);
            configReaders.setTestOnBorrow(true);
            configReaders.setBlockWhenExhausted(true);
            this.readers = new GenericKeyedObjectPool<>(new ReadersFactory(),
                    configReaders);

            this.bookKeeper = BookKeeper
                    .forConfig(clientConfiguration)
                    .build();
            this.maxWriterTtl = configuration.getWriterMaxTtl();
        } catch (IOException | InterruptedException | BKException ex) {
            throw new ObjectManagerException(ex);
        }
    }

    void scanAndDeleteLedgersForBuckets(List<BucketMetadata> buckets) throws ObjectManagerException {
        try {
            BookKeeperAdmin admin = new BookKeeperAdmin(bookKeeper);

            for (long ledgerId : admin.listLedgers()) {
                String bucketUUID;
                String bucketid;
                try (LedgerHandle lh = bookKeeper.openLedgerNoRecovery(ledgerId,
                        BookKeeper.DigestType.CRC32C, DUMMY_PWD);) {
                    LedgerMetadata ledgerMetadata = admin.getLedgerMetadata(lh);
                    Map<String, byte[]> metadata = ledgerMetadata.
                            getCustomMetadata();
                    byte[] _bucketUUid = metadata.get(BK_METADATA_BUCKET_UUID);
                    byte[] _bucketId = metadata.get(BK_METADATA_BUCKET_ID);
                    if (_bucketUUid == null || _bucketId == null) {
                        continue;
                    }
                    bucketUUID = new String(_bucketUUid, StandardCharsets.UTF_8);
                    bucketid = new String(_bucketId, StandardCharsets.UTF_8);
                }
                boolean found = buckets
                        .stream()
                        .anyMatch(b -> b.getBucketId().equals(bucketid) && b.
                        getUuid().equals(bucketUUID));
                if (found) {
                    LOG.log(Level.INFO,
                            "found droppable ledger {0}, for {1}, {2}",
                            new Object[]{ledgerId, bucketid, bucketUUID});
                    bookKeeper.deleteLedger(ledgerId);
                }
            }
        } catch (BKException | IOException | InterruptedException err) {
            throw new ObjectManagerException(err);
        }
    }

    boolean dropLedger(long idledger) throws ObjectManagerException {
        if (activeWriters.containsKey(idledger)) {
            LOG.log(Level.FINE, "cannot drop ledger used locally {0}", idledger);
            return false;
        }
        try {
            LOG.log(Level.INFO, "dropping ledger {0}", idledger);
            bookKeeper.deleteLedger(idledger);
            return true;
        } catch (BKException.BKNoSuchLedgerExistsException ok) {
            return true;
        } catch (BKException err) {
            throw new ObjectManagerException(err);
        } catch (InterruptedException err) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    @Override
    public void close() {

        writers.close();
        readers.close();

        waitForWritersTermination();

        if (bookKeeper != null) {
            try {
                bookKeeper.close();
            } catch (BKException | InterruptedException err) {
                LOG.log(Level.SEVERE, "Error while closing BK", err);
            }
        }
        threadpool.shutdown();
        callbacksExecutor.shutdown();
    }

    private void waitForWritersTermination() {

        for (BucketWriter writer : activeWriters.values()) {
            writer.awaitTermination();
        }

        while (!activeWriters.isEmpty()) {
            for (BucketWriter writer : activeWriters.values()) {
                scheduleWriterDisposal(writer);
                writer.awaitTermination();
            }
        }
    }

    Future<?> scheduleWriterDisposal(BucketWriter writer) {
        if (writer.isClosed()) {
            return FutureUtils.Void();
        }
        return threadpool.submit(() -> {
            if (writer.releaseResources()) {
                activeWriters.remove(writer.getId(), writer);
            }
        });
    }

    Future<?> scheduleReaderDisposal(BucketReader reader) {
        return threadpool.submit(() -> {
            reader.releaseResources();
        });
    }

    ExecutorService getCallbacksExecutor() {
        return callbacksExecutor;
    }

    void closeAllActiveWritersForTests() {
        List<BucketWriter> actualWriters = new ArrayList<>(activeWriters.
                values());
        writers.clear();
        for (BucketWriter writer : actualWriters) {
            writer.awaitTermination();
        }
    }

    public Stats getStats() {
        return stats;
    }

}
