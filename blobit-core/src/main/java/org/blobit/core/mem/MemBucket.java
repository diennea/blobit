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
package org.blobit.core.mem;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import org.blobit.core.api.BucketConfiguration;
import org.blobit.core.api.BucketMetadata;
import org.blobit.core.api.LedgerMetadata;
import org.blobit.core.api.ObjectManagerException;

/**
 * Implementation of an in memory bucket
 *
 * @author enrico.olivelli
 */
final class MemBucket {

    private final String bucketId;
    private final BucketConfiguration configuration;
    private final Map<Long, MemLedger> ledgers = new ConcurrentHashMap<>();
    private final ReentrantLock lockCurrentLedger = new ReentrantLock();
    private final AtomicLong currentLedger;

    public MemBucket(String name, BucketConfiguration configuration) {
        this.bucketId = name;
        this.configuration = configuration;
        this.currentLedger = new AtomicLong(0);
    }

    public BucketMetadata getMetadata() {
        return null;
    }

    public String getBucketId() {
        return bucketId;
    }

    public BucketConfiguration getConfiguration() {
        return configuration;
    }

    void registerLedger(long ledgerId) throws ObjectManagerException {
        MemLedger old = ledgers.computeIfAbsent(ledgerId, (l) -> {
            return new MemLedger(bucketId, ledgerId);
        });
        if (old == null) {
            throw new ObjectManagerException("ledger " + ledgerId + " already exists in bucket " + bucketId);
        }
    }

    MemLedger getLedger(long ledgerId) throws ObjectManagerException {
        MemLedger res = ledgers.get(ledgerId);
        if (res == null) {
            throw new ObjectManagerException("ledger does not exists in bucket " + bucketId);
        }
        return res;
    }

    void deleteLedger(long ledgerId) {
        ledgers.remove(ledgerId);
    }

    Collection<Long> listDeletableLedgers() {
        return ledgers.values().stream()
            .filter(MemLedger::isEmpty)
            .map(MemLedger::getLedgerId)
            .collect(Collectors.toList());
    }

    Collection<LedgerMetadata> listLedgers() {
        return ledgers.values().stream().map(MemLedger::getMetadata)
            .collect(Collectors.toList());
    }

    MemLedger getCurrentLedger() throws ObjectManagerException {
        lockCurrentLedger.lock();
        try {
            long current = currentLedger.get();
            if (current == 0) {
                currentLedger.set(1);
                registerLedger(1);
            }
        } finally {
            lockCurrentLedger.unlock();
        }
        return getLedger(currentLedger.get());
    }

    void gc() {
        Collection<Long> deletable = listDeletableLedgers();
        for (long id : deletable) {
            deleteLedger(id);
        }
    }


}
