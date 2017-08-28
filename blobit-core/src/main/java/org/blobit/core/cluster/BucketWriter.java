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
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.bookkeeper.client.AsyncCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BKException.BKLedgerClosedException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.blobit.core.api.ObjectManagerException;
import org.blobit.core.api.PutPromise;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Writes all data for a given bucket
 *
 * @author enrico.olivelli
 */
public class BucketWriter {

    private static final Logger LOG = Logger.getLogger(BucketWriter.class.getName());

    private final ExecutorService callbacksExecutor;
    private final String bucketId;
    private final LedgerHandle lh;
    private volatile boolean valid;
    private AtomicLong writtenBytes = new AtomicLong();
    private AtomicInteger pendingWrites = new AtomicInteger();
    private long maxBytesPerLedger;
    private final HerdDBMetadataStorageManager metadataStorageManager;
    private final BookKeeperBlobManager blobManager;
    private static final byte[] DUMMY_PWD = new byte[0];
    private static final int MAX_ENTRY_SIZE = 10 * 1024;
    private final Long id;
    private AtomicLong nextEntryId = new AtomicLong();

    public BucketWriter(String bucketId, BookKeeper bookKeeper,
        int replicationFactor,
        long maxBytesPerLedger,
        HerdDBMetadataStorageManager metadataStorageManager,
        BookKeeperBlobManager blobManager) throws ObjectManagerException {

        LOG.log(Level.FINE, "Opening BucketWriter for bucket {0}", bucketId);

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
        } catch (InterruptedException | BKException ex) {
            throw new ObjectManagerException(ex);
        }

        LOG.log(Level.INFO, "Opened BucketWriter for bucket {0}: ledger {1}, replication factor {2}", new Object[]{bucketId, id, replicationFactor});

    }

    public Long getId() {
        return id;
    }

    private class AddEntryOperation implements AsyncCallback.AddCallback {

        final CompletableFuture<Void> result;
        final AtomicInteger countRemaining;
        final int len;

        public AddEntryOperation(
            CompletableFuture<Void> result, AtomicInteger countRemaining, int len) {
            this.result = result;
            this.len = len;
            this.countRemaining = countRemaining;
        }

        @Override
        @SuppressFBWarnings("NP_NONNULL_PARAM_VIOLATION")
        public void addComplete(int rc, LedgerHandle lh1, long entryId, Object ctx) {
            boolean last = countRemaining.decrementAndGet() == 0;
            if (rc == BKException.Code.OK) {
                writtenBytes.addAndGet(len);
                if (last) {
                    pendingWrites.decrementAndGet();
                    /* NP_NONNULL_PARAM_VIOLATION: https://github.com/findbugsproject/findbugs/issues/79 */
                    result.complete(null);
                }
            } else {
                // TODO ? fail other writes?
                pendingWrites.decrementAndGet();
                BKException err = BKException.create(rc);
                LOG.log(Level.SEVERE, "bad error while adding  entry " + entryId, err);
                result.completeExceptionally(err);
            }
        }

    }

    @SuppressFBWarnings("NP_NONNULL_PARAM_VIOLATION")
    PutPromise writeBlob(String bucketId, byte[] data, int offset, int len) {
        CompletableFuture<Void> result = new CompletableFuture<>();

        pendingWrites.incrementAndGet();

        int numEntries = 1 + ((len - 1) / MAX_ENTRY_SIZE);

        long firstEntryId = nextEntryId.getAndAdd(numEntries);
        long lastEntryId = firstEntryId + numEntries - 1;

        if (len == 0) {
            /* NP_NONNULL_PARAM_VIOLATION: https://github.com/findbugsproject/findbugs/issues/79 */
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

            AddEntryOperation addEntry = new AddEntryOperation(
                result,
                remaining,
                chunkLen);

            try {
                lh.asyncAddEntry(entryId, data, chunkStartOffSet, chunkLen, addEntry, null);
            } catch (BKException neverThrown) {
            }

            chunkStartOffSet += chunkLen;
            written += chunkLen;

            entryId++;
        }

        CompletableFuture<Void> afterMetadata = result.handleAsync(
            (Void v, Throwable u) -> {
                if (u != null) {
                    throw new RuntimeException(u);
                }
                try {
                    metadataStorageManager.registerObject(bucketId, id, firstEntryId, lastEntryId, data.length);
                    return null;
                } catch (Throwable err) {
                    LOG.log(Level.SEVERE, "bad error while completing blob " + BKEntryId.formatId(lh.getId(), firstEntryId, lastEntryId), err);
                    throw new RuntimeException(err);
                }
            }, callbacksExecutor);
        return new PutPromise(BKEntryId
            .formatId(lh.getId(), firstEntryId, lastEntryId), afterMetadata);
    }

    public boolean isValid() {
        return valid
            && maxBytesPerLedger >= writtenBytes.get();
    }


    private volatile boolean closed = false;
    private final Lock closeLock = new ReentrantLock();
    private final Condition closeCompleted = closeLock.newCondition();


    public void close() {
        LOG.log(Level.SEVERE, "closing {0}", this);
        blobManager.scheduleWriterDisposal(this);
    }

    public void awaitTermination() {

        closeLock.lock();
        try {
            /* Check if the facility was closed prior to obtain the lock */
            LOG.log(Level.FINE, "Awaiting dispose {0}", this);

            while(!closed)  {

                /*
                 * Virtually release the lock to permit the real close process to take place and signal back
                 * termination
                 */
                closeCompleted.awaitUninterruptibly();

                LOG.log(Level.FINE, "Wake up on dispose {0}", this);
            }
        } finally {
            closeLock.unlock();
        }

    }

    /**
     * Release resources or schedule them to release.
     * @return {@code true} if really released, {@code false} otherwise (rescheduled or already closed)
     */
    boolean releaseResources() {
        if (pendingWrites.get() > 0) {
            LOG.log(Level.FINE, "Rescheduling for dispose {0}", this);
            blobManager.scheduleWriterDisposal(this);
            return false;
        } else {
            LOG.log(Level.INFO, "Disposing {0}", this);
            closeLock.lock();
            try {
                try {
                    if (!closed) {
                        lh.close();

                        LOG.log(Level.INFO, "Disposed {0}", this);
                        return true;
                    } else {
                        LOG.log(Level.INFO, "Already disposed {0}", this);
                    }
                } finally {
                    /* Change closing state */
                    closed = true;

                    LOG.log(Level.FINE, "Signalling disposed {0}", this);

                    /* Signal that close finished to eventual waiters */
                    closeCompleted.signalAll();
                }
                return false;
            } catch (BKLedgerClosedException err) {
                LOG.log(Level.FINE, "error while closing ledger " + lh.getId(), err);
                return true;
            } catch (BKException | InterruptedException err) {
                LOG.log(Level.SEVERE, "error while closing ledger " + lh.getId(), err);
                return true;
            } finally {
                closeLock.unlock();
            }
        }
    }

    @Override
    public String toString() {
        return "BucketWriter{" + "bucketId=" + bucketId + ", ledgerId=" + id + '}';
    }

}
