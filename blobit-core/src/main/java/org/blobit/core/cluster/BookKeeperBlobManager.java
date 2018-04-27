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

import java.io.IOException;
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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.client.DefaultEnsemblePlacementPolicy;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.LedgerMetadata;
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
import org.blobit.core.api.ObjectManagerException;
import org.blobit.core.api.PutPromise;
import static org.blobit.core.cluster.BucketWriter.BK_METADATA_BUCKET_ID;
import static org.blobit.core.cluster.BucketWriter.BK_METADATA_BUCKET_UUID;
import static org.blobit.core.cluster.BucketWriter.DUMMY_PWD;

/**
 * Stores Objects on Apache BookKeeper
 *
 * @author enrico.olivelli
 */
public class BookKeeperBlobManager implements AutoCloseable {

    private static final Logger LOG = Logger.getLogger(BookKeeperBlobManager.class.getName());
    static final String PROPERTY_BOOKKEEPER_ZK_LEDGERS_ROOT_PATH_DEFAULT = "/blobit-bk";

    private final HerdDBMetadataStorageManager metadataStorageManager;
    private final BookKeeper bookKeeper;
    final GenericKeyedObjectPool<String, BucketWriter> writers;
    final GenericKeyedObjectPool<Long, BucketReader> readers;
    private final int replicationFactor;
    private final long maxBytesPerLedger;
    private final ExecutorService callbacksExecutor;
    private final ExecutorService threadpool = Executors.newSingleThreadExecutor();
    private ConcurrentMap<Long, BucketWriter> activeWriters = new ConcurrentHashMap<>();
    private final Stats stats = new Stats();

    public static final class Stats {

        private final LongAdder usedWritersAsReaders = new LongAdder();

        public long getUsedWritersAsReaders() {
            return usedWritersAsReaders.longValue();
        }
    }

    public PutPromise put(String bucketId, byte[] data, int offset, int len) {
        if (data.length < offset + len || offset < 0 || len < 0) {
            throw new IndexOutOfBoundsException();
        }
        try {
            BucketWriter writer = writers.borrowObject(bucketId);
            try {
                return writer
                        .writeBlob(bucketId, data, offset, len);
            } finally {
                writers.returnObject(bucketId, writer);
            }
        } catch (Exception err) {
            return new PutPromise(null, wrapGenericException(err));
        }
    }

    private <T> CompletableFuture<T> wrapGenericException(Exception err) {
        CompletableFuture<T> error = new CompletableFuture<>();
        error.completeExceptionally(new ObjectManagerException(err));
        return error;
    }

    public CompletableFuture<byte[]> get(String bucketId, String id) {
        try {
            BKEntryId entry = BKEntryId.parseId(id);
            BucketReader reader = readers.borrowObject(entry.ledgerId);
            try {
                CompletableFuture<byte[]> result = reader
                        .readObject(entry.firstEntryId, entry.lastEntryId);
                return result;
            } finally {
                readers.returnObject(entry.ledgerId, reader);
            }
        } catch (Exception err) {
            return wrapGenericException(err);
        }
    }

    private final class WritersFactory implements KeyedPooledObjectFactory<String, BucketWriter> {

        @Override
        public PooledObject<BucketWriter> makeObject(String bucketId) throws Exception {
            BucketWriter writer = new BucketWriter(bucketId,
                    bookKeeper, replicationFactor, maxBytesPerLedger, metadataStorageManager, BookKeeperBlobManager.this);
            activeWriters.put(writer.getId(), writer);
            DefaultPooledObject<BucketWriter> be = new DefaultPooledObject<>(writer);
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

    private final class ReadersFactory implements KeyedPooledObjectFactory<Long, BucketReader> {

        @Override
        public PooledObject<BucketReader> makeObject(Long ledgerId) throws Exception {
            BucketReader reader;
            BucketWriter writer = activeWriters.get(ledgerId);
            if (writer != null && writer.isValid()) {
                stats.usedWritersAsReaders.increment();
                // the reader will se the LedgerHandle internal to the BucketWriter
                // as a 'reader' the LedgerHandle will continue to work even if 'closed'
                // because in BookKeeper 'closed' means something like 'sealed'
                reader = new BucketReader(writer.getLh(), BookKeeperBlobManager.this);
            } else {
                reader = new BucketReader(ledgerId, bookKeeper, BookKeeperBlobManager.this);
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

    public BookKeeperBlobManager(Configuration configuration, HerdDBMetadataStorageManager metadataStorageManager) throws ObjectManagerException {
        try {
            this.replicationFactor = configuration.getReplicationFactor();
            this.maxBytesPerLedger = configuration.getMaxBytesPerLedger();
            this.metadataStorageManager = metadataStorageManager;
            int concurrentWrites = configuration.getConcurrentWriters();
            int concurrentReaders = configuration.getMaxReaders();
            this.callbacksExecutor = Executors.newFixedThreadPool(concurrentWrites);
            ClientConfiguration clientConfiguration = new ClientConfiguration();
            clientConfiguration.setThrottleValue(0);
            clientConfiguration.setZkLedgersRootPath(PROPERTY_BOOKKEEPER_ZK_LEDGERS_ROOT_PATH_DEFAULT);
            clientConfiguration.setEnsemblePlacementPolicy(DefaultEnsemblePlacementPolicy.class);
            clientConfiguration.setLedgerManagerFactoryClass(HierarchicalLedgerManagerFactory.class);
//            clientConfiguration.setUseV2WireProtocol(true);
            for (String key : configuration.keys()) {
                if (key.startsWith("bookkeeper.")) {
                    String rawKey = key.substring("bookkeeper.".length());
                    clientConfiguration.setProperty(rawKey, configuration.getProperty(key));
                }
            }
            clientConfiguration.setZkServers(configuration.getZookkeeperUrl());
            GenericKeyedObjectPoolConfig configWriters = new GenericKeyedObjectPoolConfig();
            configWriters.setMaxTotalPerKey(concurrentWrites);
            configWriters.setMaxIdlePerKey(concurrentWrites);
            configWriters.setTestOnReturn(true);
            configWriters.setTestOnBorrow(true);
            configWriters.setBlockWhenExhausted(true);
            this.writers = new GenericKeyedObjectPool<>(new WritersFactory(), configWriters);

            GenericKeyedObjectPoolConfig configReaders = new GenericKeyedObjectPoolConfig();
            configReaders.setMaxTotalPerKey(1);
            configReaders.setMaxIdlePerKey(1);
            configReaders.setMaxTotal(concurrentReaders);
            configReaders.setTestOnReturn(true);
            configReaders.setTestOnBorrow(true);
            configReaders.setBlockWhenExhausted(true);

            this.readers = new GenericKeyedObjectPool<>(new ReadersFactory(), configReaders);

            this.bookKeeper = BookKeeper
                    .forConfig(clientConfiguration)
                    .build();
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
                try (LedgerHandle lh = bookKeeper.openLedgerNoRecovery(ledgerId, BookKeeper.DigestType.CRC32, DUMMY_PWD);) {
                    LedgerMetadata ledgerMetadata = admin.getLedgerMetadata(lh);
                    Map<String, byte[]> metadata = ledgerMetadata.getCustomMetadata();
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
                        .anyMatch(b -> b.getBucketId().equals(bucketid) && b.getUuid().equals(bucketUUID));
                if (found) {
                    LOG.log(Level.INFO, "found droppable ledger {0}, for {1}, {2}", new Object[]{ledgerId, bucketid, bucketUUID});
                    bookKeeper.deleteLedger(ledgerId);
                }
            }
        } catch (BKException | IOException | InterruptedException err) {
            throw new ObjectManagerException(err);
        }
    }

    boolean dropLedger(long idledger) throws ObjectManagerException {
        if (activeWriters.containsKey(idledger)) {
            LOG.log(Level.INFO, "cannot drop ledger used locally {0}", idledger);
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
        List<BucketWriter> actualWriters = new ArrayList<>(activeWriters.values());
        writers.clear();
        for (BucketWriter writer : actualWriters) {
            writer.awaitTermination();
        }
    }

    public Stats getStats() {
        return stats;
    }

}
