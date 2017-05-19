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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.blobit.core.api.LedgerMetadata;
import org.blobit.core.api.ObjectManagerException;
import org.blobit.core.api.ObjectMetadata;

class MemLedger {

    private static final class MemEntry {

        final MemEntryId entryId;
        final byte[] data;

        public MemEntry(MemEntryId entryId, byte[] data) {
            this.entryId = entryId;
            this.data = data;
        }

    }
    private final AtomicLong nextId = new AtomicLong();
    private final Map<Long, MemEntry> data = new ConcurrentHashMap<>();
    private final long ledgerId;
    private final String bucketId;

    public MemLedger(String bucketId, long ledgerId) {
        this.ledgerId = ledgerId;
        this.bucketId = bucketId;
    }

    public boolean isEmpty() {
        return data.isEmpty();
    }

    public long getLedgerId() {
        return ledgerId;
    }

    public String getBucketId() {
        return bucketId;
    }

    void registerObject(long entryId, long lastEntryId, long size) {

    }

    LedgerMetadata getMetadata() {
        return new LedgerMetadata(bucketId, ledgerId);
    }

    void deleteObject(long entryId) {
        data.remove(entryId);
    }

    Collection<ObjectMetadata> listObjects() {
        return data
            .entrySet()
            .stream()
            .map(entry -> {
                return new ObjectMetadata(
                    entry.getValue().entryId.toId(),
                    entry.getValue().data.length);
            })
            .collect(Collectors.toList());
    }

    byte[] get(long firstEntryId) throws ObjectManagerException {
        MemEntry res = data.get(firstEntryId);
        if (res == null) {
            throw new ObjectManagerException("no such entry " + firstEntryId);
        }
        return res.data;
    }

    public MemEntryId put(byte[] _data) {
        long id = nextId.incrementAndGet();
        MemEntryId entryId = new MemEntryId(this.ledgerId, id, id);
        data.put(id, new MemEntry(entryId, _data));
        return entryId;
    }

    void delete(long firstEntryId) {
        data.remove(firstEntryId);
    }

}
