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

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.bookkeeper.client.api.BKException;
import org.apache.bookkeeper.client.api.BookKeeper;
import org.apache.bookkeeper.client.api.LedgerEntry;

import org.blobit.core.api.ObjectManagerException;

import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.ReadHandle;

/**
 * Writes all data for a given bucket
 *
 * @author enrico.olivelli
 */
public class BucketReader {

    private static final Logger LOG = Logger.getLogger(BucketReader.class.getName());

    private final ReadHandle lh;
    private volatile boolean valid;
    private AtomicInteger pendingReads = new AtomicInteger();
    private final BookKeeperBlobManager blobManager;
    private static final byte[] DUMMY_PWD = new byte[0];

    public BucketReader(long ledgerId, BookKeeper bookKeeper,
        BookKeeperBlobManager blobManager) throws ObjectManagerException {

        LOG.log(Level.FINE, "Opening BucketReader for ledger {0}", ledgerId);

        try {
            this.blobManager = blobManager;
            this.lh = bookKeeper.newOpenLedgerOp()
                .withPassword(DUMMY_PWD)
                .withDigestType(DigestType.CRC32)
                .withLedgerId(ledgerId)
                .withRecovery(false)
                .execute()
                .get();
            valid = true;
        } catch (InterruptedException | ExecutionException ex) {
            throw new ObjectManagerException(ex);
        }

        LOG.log(Level.INFO, "Opened BucketReader for ledger {0}", ledgerId);
    }

    public CompletableFuture<byte[]> readObject(long entryId, long last) {

        pendingReads.incrementAndGet();
        return lh.readUnconfirmedAsync(entryId, last)
            .handle((Iterable<org.apache.bookkeeper.client.api.LedgerEntry> entries, Throwable u) -> {
                pendingReads.decrementAndGet();
                if (u != null) {
                    valid = false;
                    throw new RuntimeException(u);
                }
                int size = 0;
                List<ByteBuf> buffers = new ArrayList<>((int) (1 + last - entryId));
                for (LedgerEntry entry : entries) {
                    ByteBuf buf = entry.getEntryBuffer();
                    size += buf.readableBytes();
                    buffers.add(buf);
                }

                final byte[] data = new byte[size];
                int offset = 0;
                for (ByteBuf buf : buffers) {
                    int readable = buf.readableBytes();
                    buf.readBytes(data, offset, readable);
                    offset += readable;
                    buf.release();
                }

                return data;
            });
    }

    public boolean isValid() {
        return valid;
    }

    public void close() {
        LOG.log(Level.SEVERE, "closing {0}", this);
        blobManager.scheduleReaderDisposal(this);
    }

    void releaseResources() {
        if (pendingReads.get() > 0) {
            blobManager.scheduleReaderDisposal(this);
        } else {
            try {
                lh.close();
            } catch (BKException | InterruptedException err) {
                LOG.log(Level.SEVERE, "error while closing ledger " + lh.getId(), err);
            }
        }
    }

    @Override
    public String toString() {
        return "BucketReader{" + "ledgerId=" + lh.getId() + '}';
    }

}
