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

import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.util.Observable;
import java.util.Observer;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.CheckpointSource;
import org.apache.bookkeeper.bookie.LedgerDirsManager;
import org.apache.bookkeeper.bookie.LedgerStorage;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.stats.StatsLogger;

/**
 *
 * @author enrico.olivelli
 */
public class NullLedgerStore implements LedgerStorage {

    @Override
    public void initialize(ServerConfiguration conf, LedgerManager ledgerManager, LedgerDirsManager ledgerDirsManager, LedgerDirsManager indexDirsManager, CheckpointSource checkpointSource, StatsLogger statsLogger) throws IOException {

    }

    @Override
    public void start() {

    }

    @Override
    public void shutdown() throws InterruptedException {

    }

    @Override
    public boolean ledgerExists(long ledgerId) throws IOException {
        return true;
    }

    @Override
    public boolean setFenced(long ledgerId) throws IOException {
        return false;
    }

    @Override
    public boolean isFenced(long ledgerId) throws IOException {
        return false;
    }

    @Override
    public void setMasterKey(long ledgerId, byte[] masterKey) throws IOException {

    }

    @Override
    public byte[] readMasterKey(long ledgerId) throws IOException, BookieException {
        return new byte[0];
    }

    @Override
    public long addEntry(ByteBuf entry) throws IOException {
        long ledgerId = entry.getLong(entry.readerIndex() + 0);
        long entryId = entry.getLong(entry.readerIndex() + 8);
        long lac = entry.getLong(entry.readerIndex() + 16);
        return entryId;
    }

    @Override
    public ByteBuf getEntry(long ledgerId, long entryId) throws IOException {
        return null;
    }

    @Override
    public long getLastAddConfirmed(long ledgerId) throws IOException {
        return -1;
    }

    @Override
    public Observable waitForLastAddConfirmedUpdate(long ledgerId, long previoisLAC, Observer observer) throws IOException {
        return null;
    }

    @Override
    public void flush() throws IOException {

    }

    @Override
    public CheckpointSource.Checkpoint checkpoint(CheckpointSource.Checkpoint checkpoint) throws IOException {
        return checkpoint;
    }

    @Override
    public void deleteLedger(long ledgerId) throws IOException {

    }

    @Override
    public void registerLedgerDeletionListener(LedgerDeletionListener listener) {

    }

    @Override
    public void setExplicitlac(long ledgerId, ByteBuf lac) throws IOException {

    }

    @Override
    public ByteBuf getExplicitLac(long ledgerId) {
        return null;
    }

}
