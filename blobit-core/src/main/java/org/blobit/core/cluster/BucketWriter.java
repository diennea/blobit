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
import io.netty.buffer.Unpooled;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.bookkeeper.client.api.BKException;
import org.apache.bookkeeper.client.api.BookKeeper;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.WriteAdvHandle;
import org.apache.bookkeeper.client.api.WriteFlag;
import org.blobit.core.api.BucketMetadata;
import org.blobit.core.api.ObjectManagerException;
import org.blobit.core.api.ObjectManagerRuntimeException;
import org.blobit.core.api.PutOptions;
import org.blobit.core.api.PutPromise;

/**
 * Writes all data for a given bucket
 *
 * @author enrico.olivelli
 */
public class BucketWriter {

    private static final Logger LOG = Logger.getLogger(BucketWriter.class.
            getName());
    static final String BK_METADATA_BUCKET_ID = "bucketId";
    static final String BK_METADATA_BUCKET_UUID = "bucketUUID";
    static final String BK_METADATA_APPLICATION_NAME = "application";
    static final byte[] BK_METADATA_APPLICATION_NAME_VALUE = "blobit".getBytes(StandardCharsets.UTF_8);
    static final String BK_METADATA_CREATOR = "creator";
    static final byte[] BK_METADATA_CREATOR_VALUE;
    static {
        String creator = computeCreatorValue();
        BK_METADATA_CREATOR_VALUE = creator.getBytes(StandardCharsets.UTF_8);
    }

    protected static String computeCreatorValue() {
        String creator = null;
        try {
            creator = InetAddress.getLocalHost().getCanonicalHostName();
        } catch (Throwable t) {
            LOG.log(Level.SEVERE, "Cannot get localhost name", t);
            try {
                creator = InetAddress.getLocalHost().getHostAddress();
            } catch (Throwable t2) {
                LOG.log(Level.SEVERE, "Cannot get localhost ip", t2);
            }
        }
        if (creator == null) {
            creator = "localhost";
        }
        return creator;
    }

    private final ExecutorService callbacksExecutor;
    private final String bucketId;
    private final WriteAdvHandle lh;
    private volatile boolean valid;
    private AtomicLong writtenBytes = new AtomicLong();
    private AtomicInteger pendingWrites = new AtomicInteger();
    private final long maxBytesPerLedger;
    private final int maxEntrySize;
    private final boolean deferredSync;
    private final HerdDBMetadataStorageManager metadataStorageManager;
    private final BookKeeperBlobManager blobManager;
    static final byte[] DUMMY_PWD = new byte[0];

    private static final EnumSet<WriteFlag> DEFERRED_SYNC = EnumSet.of(
            WriteFlag.DEFERRED_SYNC);
    private final Long id;
    private AtomicLong nextEntryId = new AtomicLong();
    private final long maxValidTime;

    public BucketWriter(String bucketId,
            BookKeeper bookKeeper,
            int replicationFactor,
            int maxEntrySize,
            long maxBytesPerLedger,
            boolean enableChecksum,
            boolean deferredSync,
            HerdDBMetadataStorageManager metadataStorageManager,
            BookKeeperBlobManager blobManager,
            long maxValidTime) throws ObjectManagerException {

        LOG.log(Level.FINE, "Opening BucketWriter for bucket {0}", bucketId);

        try {
            this.maxValidTime = maxValidTime;
            this.maxEntrySize = maxEntrySize;
            this.blobManager = blobManager;
            this.callbacksExecutor = blobManager.getCallbacksExecutor();
            this.maxBytesPerLedger = maxBytesPerLedger;
            this.metadataStorageManager = metadataStorageManager;
            BucketMetadata bucketMetadata = metadataStorageManager.
                    getBucketMetadata(bucketId);
            if (bucketMetadata == null) {
                throw new ObjectManagerException("no such bucket " + bucketId);
            }
            String bucketUUID = bucketMetadata.getUuid();
            this.bucketId = bucketId;
            Map<String, byte[]> ledgerMetadata = new HashMap<>();
            ledgerMetadata.put(BK_METADATA_BUCKET_ID, bucketId.getBytes(
                    StandardCharsets.UTF_8));
            ledgerMetadata.put(BK_METADATA_BUCKET_UUID, bucketUUID.getBytes(
                    StandardCharsets.UTF_8));
            ledgerMetadata.put(BK_METADATA_APPLICATION_NAME, BK_METADATA_APPLICATION_NAME_VALUE);
            ledgerMetadata.put(BK_METADATA_CREATOR, BK_METADATA_CREATOR_VALUE);
            this.deferredSync = deferredSync;
            this.lh = bookKeeper.
                    newCreateLedgerOp()
                    .withAckQuorumSize(replicationFactor)
                    .withWriteQuorumSize(replicationFactor)
                    .withEnsembleSize(replicationFactor)
                    .withDigestType(
                            enableChecksum ? DigestType.CRC32C : DigestType.DUMMY).
                    withWriteFlags(deferredSync ? DEFERRED_SYNC : WriteFlag.NONE).
                    withPassword(DUMMY_PWD)
                    .withCustomMetadata(ledgerMetadata)
                    .makeAdv()
                    .execute()
                    .get();
            valid = true;
            this.id = lh.getId();
            metadataStorageManager.registerLedger(bucketId, this.id);
        } catch (InterruptedException ex) {
            throw new ObjectManagerException(ex);
        } catch (ExecutionException ex) {
            throw new ObjectManagerException(ex.getCause());
        }

        LOG.log(Level.INFO,
                "Opened BucketWriter for bucket {0}: ledger {1}, replication factor {2}",
                new Object[]{bucketId, id, replicationFactor});

    }

    WriteAdvHandle getLh() {
        return lh;
    }

    public Long getId() {
        return id;
    }

    @SuppressFBWarnings("NP_NONNULL_PARAM_VIOLATION")
    PutPromise writeBlob(String bucketId, String name, byte[] data, int offset,
            int len, PutOptions putOptions) {

        if (len == 0) {
            CompletableFuture<Void> result = new CompletableFuture<>();
            result.completeExceptionally(new IllegalStateException().
                    fillInStackTrace());
            return new PutPromise(null, result);
        }

        int numEntries = 1 + ((len - 1) / maxEntrySize);

        long firstEntryId = nextEntryId.getAndAdd(numEntries);
        String blobId = BKEntryId
                .formatId(id, firstEntryId, maxEntrySize, len, numEntries);

        pendingWrites.incrementAndGet();

        int chunkStartOffSet = 0;
        int chunkLen = maxEntrySize;
        int written = 0;
        long entryId = firstEntryId;
        CompletableFuture<Long> lastEntry = null;
        for (int i = 0; i < numEntries; i++) {
            if (len <= maxEntrySize) {
                // very small entry
                chunkLen = len;
            } else if (written + maxEntrySize >= len) {
                // last
                chunkLen = len - written;
            }
            writtenBytes.addAndGet(chunkLen);
            lastEntry = lh.writeAsync(entryId, Unpooled.wrappedBuffer(data,
                    chunkStartOffSet, chunkLen));
            chunkStartOffSet += chunkLen;
            written += chunkLen;
            entryId++;
        }

        if (lastEntry == null) {
            pendingWrites.decrementAndGet();
            CompletableFuture<Void> result = new CompletableFuture<>();
            result.completeExceptionally(new IllegalStateException().
                    fillInStackTrace());
            return new PutPromise(null, result);
        }

        // we are attaching to lastEntry, because BookKeeper will ackknowledge writes in order
        CompletableFuture<Long> afterMetadata = lastEntry.handleAsync(
                (Long _entryId, Throwable u) -> {
                    pendingWrites.decrementAndGet();
                    if (u != null) {
                        if (u instanceof ObjectManagerException) {
                            throw new ObjectManagerRuntimeException(u);
                        } else {
                          throw new ObjectManagerRuntimeException(
                                new ObjectManagerException(u));
                        }
                    }
                    try {
                        metadataStorageManager.registerObject(bucketId, id,
                                firstEntryId, numEntries, maxEntrySize, len,
                                blobId, name, putOptions.isOverwrite(), putOptions.isAppend());
                        return null;
                    } catch (ObjectManagerException err) {
                        throw new ObjectManagerRuntimeException(err);
                    } catch (Throwable err) {
                        LOG.log(Level.SEVERE, "bad error while completing blob", err);
                        throw new ObjectManagerRuntimeException(err);
                    }
                }, callbacksExecutor);
        return new PutPromise(blobId, afterMetadata);
    }

    @SuppressFBWarnings("NP_NONNULL_PARAM_VIOLATION")
    PutPromise writeBlob(String bucketId, String name, long len, InputStream in, PutOptions putOptions) {

        if (len == 0) {
            CompletableFuture<Void> result = new CompletableFuture<>();
            result.completeExceptionally(new IllegalStateException().
                    fillInStackTrace());
            return new PutPromise(null, result);
        }

        long _numEntries = 1 + ((len - 1) / maxEntrySize);
        if (_numEntries >= Integer.MAX_VALUE) {
            // very huge !
            CompletableFuture<Void> result = new CompletableFuture<>();
            result.completeExceptionally(new IllegalArgumentException().
                    fillInStackTrace());
            return new PutPromise(null, result);
        }

        int numEntries = (int) _numEntries;

        long firstEntryId = nextEntryId.getAndAdd(numEntries);

        final String blobId = BKEntryId
                .formatId(id, firstEntryId, maxEntrySize, len, numEntries);
        pendingWrites.incrementAndGet();
        int chunkLen = maxEntrySize;
        int written = 0;
        long entryId = firstEntryId;
        CompletableFuture<Long> lastEntry = null;

        PutPromise failedWrite = null;

        for (int i = 0; i < numEntries; i++) {
            byte[] chunk;
            if (failedWrite != null) {
                chunk = BookKeeperBlobManager.EMPTY_BYTE_ARRAY;
            } else {
                if (len <= maxEntrySize) {
                    // very small entry
                    chunkLen = (int) len;
                } else if (written + maxEntrySize >= len) {
                    // last
                    chunkLen = (int) (len - written);
                }

                // unfortunately we have to create a byte[] for each chunk
                // we will not create a single huge byte[]
                // because it the object could be larger that 2 GB
                chunk = new byte[chunkLen];
                try {
                    int n = 0;
                    while (n < chunkLen) {
                        int count = in.read(chunk, 0 + n, chunkLen - n);
                        if (count < 0) {
                            throw new EOFException(
                                    "short read from stream, read up to " + n + " expected " + chunkLen + " for chunk #"
                                    + i);
                        }
                        n += count;
                    }
                } catch (IOException err) {
                    pendingWrites.decrementAndGet();
                    CompletableFuture<Void> result = new CompletableFuture<>();
                    result.completeExceptionally(err);
                    failedWrite = new PutPromise(null, result);
                    // WE MUST NOT EXIT THE LOOP
                    // Bookkeeper is expecting all the pre-allocated entryIds
                    // to be used: you cannot leave holes in the sequence
                }
            }

            writtenBytes.addAndGet(chunkLen);
            lastEntry = lh.writeAsync(entryId, Unpooled.wrappedBuffer(chunk));
            written += chunkLen;
            entryId++;
        }

        if (failedWrite != null) {
            return failedWrite;
        }

        if (lastEntry == null) {
            pendingWrites.decrementAndGet();
            CompletableFuture<Void> result = new CompletableFuture<>();
            result.completeExceptionally(new IllegalStateException().
                    fillInStackTrace());
            return new PutPromise(blobId, result);
        }

        // we are attaching to lastEntry, because BookKeeper will ackknowledge writes in order
        CompletableFuture<Long> afterMetadata = lastEntry.handleAsync(
                (Long _entryId, Throwable u) -> {
                    int after = pendingWrites.decrementAndGet();

                    if (u != null) {
                        throw new RuntimeException(u);
                    }
                    try {
                        metadataStorageManager
                                .registerObject(bucketId, id, firstEntryId,
                                        numEntries, maxEntrySize, len, blobId,
                                        name, putOptions.isOverwrite(), putOptions.isAppend());
                        return null;
                    } catch (Throwable err) {
                        LOG.log(Level.SEVERE, "bad error while completing blob",
                                err);
                        throw new RuntimeException(err);
                    }
                }, callbacksExecutor);
        return new PutPromise(blobId, afterMetadata);
    }

    public boolean isValid() {
        return valid
                && maxBytesPerLedger >= writtenBytes.get()
                && System.currentTimeMillis() <= maxValidTime;
    }

    private volatile boolean closed = false;
    private final Lock closeLock = new ReentrantLock();
    private final Condition closeCompleted = closeLock.newCondition();

    public void close() {
        LOG.log(Level.FINER, "closing {0}", this);
        blobManager.scheduleWriterDisposal(this);
    }

    public void awaitTermination() {

        closeLock.lock();
        try {
            /* Check if the facility was closed prior to obtain the lock */
            LOG.log(Level.FINE, "Awaiting dispose {0}", this);

            while (!closed) {

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

    public boolean isClosed() {
        return closed;
    }

    /**
     * Release resources or schedule them to release.
     *
     * @return {@code true} if really released, {@code false} otherwise (rescheduled or already closed)
     */
    boolean releaseResources() {
        if (pendingWrites.get() > 0) {
            LOG.log(Level.FINE, "Rescheduling for dispose {0}", this);
            blobManager.scheduleWriterDisposal(this);
            return false;
        } else {
            LOG.log(Level.FINE, "Disposing {0}", this);
            closeLock.lock();
            try {
                try {
                    if (!closed) {
                        forceAndCloseHandle();

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
            } finally {
                closeLock.unlock();
            }
        }
    }

    private void forceAndCloseHandle() {
        if (deferredSync) {
            try {
                // trying to "force" the handle
                // this is like an fsync
                // it waits for an fsync on all of the journals
                // and advances LAC up to the last written entry
                // it
                lh.force().handle((res, error) -> {
                    if (error != null) {
                        LOG.log(Level.SEVERE,
                                "Error while forcing final sync on ledger " + lh.
                                        getId(), error);
                    }
                    // even in case of error we are trying to close the
                    closeHandle();
                    return res;
                }).get();
            } catch (ExecutionException notReallyAProblem) {
                LOG.log(Level.INFO,
                        "There was an error while closing ledger " + id + ", this should not be a big problem",
                        notReallyAProblem);
            } catch (InterruptedException notReallyAProblem) {
                Thread.currentThread().interrupt();
                LOG.log(Level.INFO,
                        "There was an error while closing ledger " + id + ", this should not be a big problem",
                        notReallyAProblem);
            }
        } else {
            closeHandle();
        }
    }

    private void closeHandle() {
        // in BK to "close" an handle means to "seal" it
        // it is not a matter of returning resources to the system
        // but to write on metadata that the ledger has been written
        // safely/completely
        try {
            lh.close();
        } catch (BKException notReallyAProblem) {
            LOG.log(Level.INFO,
                    "There was an error while closing ledger " + id + ", this should not be a big problem",
                    notReallyAProblem);
        } catch (InterruptedException notReallyAProblem) {
            Thread.currentThread().interrupt();
            LOG.log(Level.INFO,
                    "There was an error while closing ledger " + id + ", this should not be a big problem",
                    notReallyAProblem);
        }
    }

    @Override
    public String toString() {
        return "BucketWriter{" + "bucketId=" + bucketId + ", ledgerId=" + id + '}';
    }

}
