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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.bookkeeper.client.AsyncCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BKException.BKLedgerClosedException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.blobit.core.api.ObjectManagerException;
import org.blobit.core.api.MetadataManager;

/**
 * Writes all data for a given bucket
 *
 * @author enrico.olivelli
 */
public class BucketWriter {

    private final String bucketId;
    private final LedgerHandle lh;
    private volatile boolean valid;
    private AtomicLong writtenBytes = new AtomicLong();
    private AtomicInteger pendingWrites = new AtomicInteger();
    private long maxBytesPerLedger;
    private final MetadataManager metadataStorageManager;
    private final BookKeeperBlobManager blobManager;
    private static final byte[] DUMMY_PWD = new byte[0];
    private static final int MAX_ENTRY_SIZE = 1024 * 1024;
    private final Long id;
    private AtomicInteger nextEntryId = new AtomicInteger();

    public BucketWriter(String bucketId, BookKeeper bookKeeper,
        int replicationFactor,
        long maxBytesPerLedger,
        MetadataManager metadataStorageManager,
        BookKeeperBlobManager blobManager) throws ObjectManagerException {
        try {
            this.blobManager = blobManager;
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

    private class AddEntryCallback implements AsyncCallback.AddCallback {

        final CompletableFuture<BKEntryId> result;
        final byte[] data;
        final int offset;
        final int end;
        final long entryId;
        final BKEntryId blobId;

        public AddEntryCallback(long entryId, CompletableFuture<BKEntryId> result, byte[] data, int offset, int end,
            BKEntryId blobId) {
            this.result = result;
            this.data = data;
            this.offset = offset;
            this.end = end;
            this.blobId = blobId;
            this.entryId = entryId;
        }

        @Override
        public void addComplete(int rc, LedgerHandle lh1, long entryId, Object ctx) {
            if (rc == BKException.Code.OK) {
                try {
                    writtenBytes.addAndGet(data.length);
                    // ISSUE: is it better to write to storage manager on other thread ?
                    boolean last = entryId == blobId.lastEntryId;
                    if (last) {
                        pendingWrites.decrementAndGet();
                        metadataStorageManager.registerObject(bucketId, lh1.getId(),
                            blobId.firstEntryId, blobId.lastEntryId, data.length);
                        result.complete(blobId);
                    } else {
                        int nextEntryOffset = offset + MAX_ENTRY_SIZE;
                        int nextEntryEnd = end + MAX_ENTRY_SIZE;
                        if (nextEntryEnd > data.length) {
                            nextEntryEnd = data.length;
                        }
                        lh.asyncAddEntry(this.entryId + 1, data, nextEntryOffset, nextEntryEnd - nextEntryOffset,
                            new AddEntryCallback(this.entryId + 1, result, data, nextEntryOffset, nextEntryEnd, blobId), null);
                    }
                } catch (ObjectManagerException | BKException err) {
                    LOG.log(Level.SEVERE, "bad error while scheduling next add entry " + (this.entryId + 1), err);
                    result.completeExceptionally(err);
                }
            } else {
                pendingWrites.decrementAndGet();
                LOG.log(Level.SEVERE, "bad error while adding  entry " + (this.entryId), BKException.create(rc).fillInStackTrace());
                result.completeExceptionally(BKException.create(rc).fillInStackTrace());
            }
        }

    }

    CompletableFuture<BKEntryId> writeBlob(String bucketId, byte[] data) {
        CompletableFuture<BKEntryId> result = new CompletableFuture<>();

        pendingWrites.incrementAndGet();

        int countEntries = 1 + ((data.length - 1) / MAX_ENTRY_SIZE);

        int firstEntryId = nextEntryId.getAndAdd(countEntries);
        int lastEntryId = firstEntryId + countEntries - 1;

        BKEntryId blobId = new BKEntryId(lh.getId(), firstEntryId, lastEntryId);

        int offset = 0;
        int end = offset + MAX_ENTRY_SIZE;
        if (end > data.length) {
            end = data.length;
        }

        try {
            lh.asyncAddEntry(firstEntryId, data, offset, end - offset,
                new AddEntryCallback(firstEntryId, result, data, offset, end, blobId), null);
        } catch (BKException er) {
            LOG.log(Level.SEVERE, "bad error while adding first entry " + firstEntryId, er);
            result.completeExceptionally(er);
        }

        return result;
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
