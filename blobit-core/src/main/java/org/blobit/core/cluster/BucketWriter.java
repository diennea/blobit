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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.bookkeeper.client.AsyncCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BKException.BKLedgerClosedException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.blobit.core.api.MetadataManager;
import org.blobit.core.api.ObjectManagerException;
import org.blobit.core.api.PutPromise;

/**
 * Writes all data for a given bucket
 *
 * @author enrico.olivelli
 */
public class BucketWriter {

    private final ExecutorService callbacksExecutor;
    private final String bucketId;
    private final LedgerHandle lh;
    private volatile boolean valid;
    private AtomicLong writtenBytes = new AtomicLong();
    private AtomicInteger pendingWrites = new AtomicInteger();
    private long maxBytesPerLedger;
    private final MetadataManager metadataStorageManager;
    private final BookKeeperBlobManager blobManager;
    private static final byte[] DUMMY_PWD = new byte[0];
    private static final int MAX_ENTRY_SIZE = 10 * 1024;
    private final Long id;
    private AtomicLong nextEntryId = new AtomicLong();

    public BucketWriter(String bucketId, BookKeeper bookKeeper,
        int replicationFactor,
        long maxBytesPerLedger,
        MetadataManager metadataStorageManager,
        BookKeeperBlobManager blobManager) throws ObjectManagerException {
        try {
            this.blobManager = blobManager;
            this.callbacksExecutor = blobManager.getCallbacksExecutor();
            this.maxBytesPerLedger = maxBytesPerLedger;
            this.metadataStorageManager = metadataStorageManager;
            this.bucketId = bucketId;
            this.lh = bookKeeper.createLedgerAdv(replicationFactor,
                replicationFactor,
                replicationFactor,
                BookKeeper.DigestType.CRC32, DUMMY_PWD);
            valid = true;
            this.id = lh.getId();
            metadataStorageManager.registerLedger(bucketId, this.id);
            LOG.log(Level.INFO, "Created new writer replicationFactor " + replicationFactor + ", for {0}: ledgerId {1}", new Object[]{bucketId, lh.getId()});
        } catch (InterruptedException | BKException ex) {
            throw new ObjectManagerException(ex);
        }

    }

    public Long getId() {
        return id;
    }

    private class AddEntryOperation implements AsyncCallback.AddCallback {

        final long entryId;
        final CompletableFuture<Void> result;
        final byte[] data;
        final int offset;
        final int len;
        final BKEntryId blobId;
        final AtomicInteger countRemaining;

        public AddEntryOperation(
            long entryId,
            CompletableFuture<Void> result, AtomicInteger countRemaining,
            byte[] data, int offset, int len, BKEntryId blobId) {
            this.entryId = entryId;
            this.result = result;
            this.data = data;
            this.offset = offset;
            this.len = len;
            this.blobId = blobId;
            this.countRemaining = countRemaining;
        }

        @Override
        public void addComplete(int rc, LedgerHandle lh1, long entryId, Object ctx) {
            boolean last = countRemaining.decrementAndGet() == 0;
            if (rc == BKException.Code.OK) {
                writtenBytes.addAndGet(len);
                if (last) {
                    pendingWrites.decrementAndGet();
                    result.complete(null);
                }
            } else {
                // TODO ? fail other writes?
                pendingWrites.decrementAndGet();
                LOG.log(Level.SEVERE, "bad error while adding  entry " + entryId, BKException.create(rc).fillInStackTrace());
                result.completeExceptionally(BKException.create(rc).fillInStackTrace());
            }
        }

        private void initiate(LedgerHandle lh) {
            try {
                lh.asyncAddEntry(entryId, data, offset, len, this, null);
            } catch (BKException neverThrown) {
            }
        }

        @Override
        public String toString() {
            return "AddEntryCallback{data=" + data + ", offset=" + offset + ", len=" + len + ", countDone=" + countRemaining + '}';
        }

    }

    PutPromise writeBlob(String bucketId, byte[] data, int offset, int len) {
        CompletableFuture<Void> result = new CompletableFuture<>();

        pendingWrites.incrementAndGet();

        int numEntries = 1 + ((len - 1) / MAX_ENTRY_SIZE);

        long firstEntryId = nextEntryId.getAndAdd(numEntries);
        long lastEntryId = firstEntryId + numEntries - 1;

        BKEntryId blobId = new BKEntryId(lh.getId(), firstEntryId, lastEntryId);

        if (len == 0) {
            result.complete(null);
            return new PutPromise(bucketId, result);
        }

        int chunkStartOffSet = 0;
        int chunkLen = MAX_ENTRY_SIZE;
        int written = 0;
        long entryId = firstEntryId;
        AtomicInteger remaining = new AtomicInteger(numEntries);
        for (int i = 0; i < numEntries; i++) {

            if (len <= MAX_ENTRY_SIZE) {
                // very small entry
                chunkLen = len;
            } else if (written + MAX_ENTRY_SIZE >= len) {
                // last
                chunkLen = len - written;
            }
            AddEntryOperation addEntry = new AddEntryOperation(entryId,
                result,
                remaining,
                data,
                chunkStartOffSet,
                chunkLen,
                blobId);
            chunkStartOffSet += chunkLen;
            written += chunkLen;

            addEntry.initiate(lh);
            entryId++;
        }

        CompletableFuture<Void> afterMetadata = result.handleAsync(
            (Void v, Throwable u) -> {
                if (u != null) {
                    throw new RuntimeException(u);
                }
                try {
                    metadataStorageManager.registerObject(bucketId, blobId.ledgerId, blobId.firstEntryId, blobId.lastEntryId, data.length);
                    return (Void) null;
                } catch (Throwable err) {
                    LOG.log(Level.SEVERE, "bad error while completing blob " + blobId, err);
                    throw new RuntimeException(err);
                }
            }, callbacksExecutor);
        return new PutPromise(blobId.toId(), afterMetadata);
    }

    private void writeBlob(CompletableFuture<Void> result, BKEntryId blobId, long entryId, byte[] data,
        int offset, int len) throws BKException {

    }

    public boolean isValid() {
        return valid
            && maxBytesPerLedger >= writtenBytes.get();
    }

    public void close() {
        LOG.log(Level.SEVERE, "closing {0}", this);
        blobManager.scheduleWriterDisposal(this);
    }

    void releaseResources() {
        if (pendingWrites.get() > 0) {
            blobManager.scheduleWriterDisposal(this);
        } else {
            LOG.log(Level.INFO, "Disposing {0}", this);
            try {
                lh.close();
            } catch (BKLedgerClosedException err) {
                LOG.log(Level.FINE, "error while closing ledger " + lh.getId(), err);
            } catch (BKException | InterruptedException err) {
                LOG.log(Level.SEVERE, "error while closing ledger " + lh.getId(), err);
            }
        }
    }

    private static final Logger LOG = Logger.getLogger(BucketWriter.class.getName());

    private static final byte[] empty = new byte[0];

    @Override
    public String toString() {
        return "BucketWriter{" + "bucketId=" + bucketId + ", id=" + id + '}';
    }

}
