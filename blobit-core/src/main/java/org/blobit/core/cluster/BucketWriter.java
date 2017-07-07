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
import org.blobit.core.api.MetadataManager;
import org.blobit.core.api.ObjectManagerException;

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
    private AtomicLong nextEntryId = new AtomicLong();

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

        final CompletableFuture<String> result;
        final byte[] data;
        final int offset;
        final int len;
        final int chunk;
        final BKEntryId blobId;

        public AddEntryCallback(
                CompletableFuture<String> result, byte[] data, int offset, int len, int chunk, BKEntryId blobId) {
            this.result = result;
            this.data = data;
            this.offset = offset;
            this.len = len;
            this.chunk = chunk;
            this.blobId = blobId;
        }

        @Override
        public void addComplete(int rc, LedgerHandle lh1, long entryId, Object ctx) {
            if (rc == BKException.Code.OK) {
                    writtenBytes.addAndGet(chunk);
                    // ISSUE: is it better to write to storage manager on other thread ?
                    boolean last = entryId == blobId.lastEntryId;
                    if (last) {
                        try {
                            pendingWrites.decrementAndGet();
                            metadataStorageManager.registerObject(
                                    bucketId, lh1.getId(), blobId.firstEntryId, blobId.lastEntryId, data.length);
                            result.complete(blobId.toId());
                        } catch (ObjectManagerException err) {
                            LOG.log(Level.SEVERE, "bad error while completing blob " + blobId, err);
                            result.completeExceptionally(err);
                        }
                    } else {
                        long nextEntry = entryId + 1;
                        try {
                            writeBlob(result, blobId, nextEntry, data, offset + chunk, len - chunk);
                        } catch (BKException err) {
                            LOG.log(Level.SEVERE, "bad error while scheduling next add entry " + nextEntry, err);
                            result.completeExceptionally(err);
                        }
                    }
            } else {
                pendingWrites.decrementAndGet();
                LOG.log(Level.SEVERE, "bad error while adding  entry " + entryId, BKException.create(rc).fillInStackTrace());
                result.completeExceptionally(BKException.create(rc).fillInStackTrace());
            }
        }

    }
   
    CompletableFuture<String> writeBlob(String bucketId, byte[] data, int offset, int len) {
        CompletableFuture<String> result = new CompletableFuture<>();

        pendingWrites.incrementAndGet();

        int countEntries = 1 + ((len - 1) / MAX_ENTRY_SIZE);

        long firstEntryId = nextEntryId.getAndAdd(countEntries);
        long lastEntryId = firstEntryId + countEntries - 1;

        BKEntryId blobId = new BKEntryId(lh.getId(), firstEntryId, lastEntryId);

        try {

            writeBlob(result, blobId, firstEntryId, data, offset, len);

        } catch (BKException er) {
            LOG.log(Level.SEVERE, "bad error while adding first entry " + firstEntryId, er);
            result.completeExceptionally(er);
        }

        return result;
    }

    private void writeBlob(CompletableFuture<String> result, BKEntryId blobId, long entryId, byte[] data,
            int offset, int len) throws BKException {
        int write = len > MAX_ENTRY_SIZE ? MAX_ENTRY_SIZE : len;
        lh.asyncAddEntry(entryId, data, offset, write,
            new AddEntryCallback(result, data, offset, len, write, blobId), null);
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
