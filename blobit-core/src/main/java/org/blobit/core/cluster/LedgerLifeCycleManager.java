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

import java.util.Collection;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.blobit.core.api.DataManagerException;
import org.blobit.core.api.BucketMetadata;
import org.blobit.core.api.MetadataManager;

/**
 * Manages lifecycle of ledgers
 *
 * @author enrico.olivelli
 */
public class LedgerLifeCycleManager {

    private static final Logger LOG = Logger.getLogger(LedgerManagerRuntime.class.getName());

    private final MetadataManager metadataStorageManager;
    private final BookKeeperBlobManager manager;
    private final ScheduledExecutorService ledgerManager;

    public LedgerLifeCycleManager(MetadataManager metadataStorageManager,
        BookKeeperBlobManager manager) {
        this.metadataStorageManager = metadataStorageManager;
        this.manager = manager;
        this.ledgerManager = Executors.newSingleThreadScheduledExecutor();
    }

    void run() {
        try {
            Collection<BucketMetadata> buckets = metadataStorageManager.listBuckets();
            for (BucketMetadata bucket : buckets) {
                String bucketId = bucket.getBucketId();
                gcBucket(bucketId);
            }
        } catch (DataManagerException ex) {
            LOG.log(Level.SEVERE, "Error during ledger management", ex);
        }
    }

    public void gcBucket(String bucketId) throws DataManagerException {
        Collection<Long> ledgers = metadataStorageManager.listDeletableLedgers(bucketId);
        LOG.log(Level.SEVERE, "There are " + ledgers.size() + " deletable ledgers");
        for (long idledger : ledgers) {
            boolean ok = manager.dropLedger(idledger);
            if (ok) {
                metadataStorageManager.deleteLedger(bucketId, idledger);
            } else {
                LOG.log(Level.SEVERE, "Drop ledger " + idledger + " failed");
            }
        }
    }

    private class LedgerManagerRuntime implements Runnable {

        @Override
        public void run() {
            try {
                LedgerLifeCycleManager.this.run();
            } catch (Throwable ex) {
                LOG.log(Level.SEVERE, "Error during ledger management", ex);
            }

        }

    }

    public void start() {
        this.ledgerManager.scheduleWithFixedDelay(new LedgerManagerRuntime(), 60, 60, TimeUnit.SECONDS);
    }

    public void close() {
        this.ledgerManager.shutdown();
    }

}
