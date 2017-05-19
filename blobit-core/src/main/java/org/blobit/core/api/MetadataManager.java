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
package org.blobit.core.api;

import java.util.Collection;

/**
 * Stores Metadata about Objects and Ledgers
 *
 * @author enrico.olivelli
 */
public interface MetadataManager {

    public void createBucket(String name,
        String bucketTableSpaceName,
        BucketConfiguration configuration) throws ObjectManagerException;

    public void init() throws ObjectManagerException;

    public Collection<BucketMetadata> listBuckets() throws ObjectManagerException;

    public void registerLedger(String bucketId, long ledgerId) throws ObjectManagerException;

    public void deleteLedger(String bucketId, long ledgerId) throws ObjectManagerException;

    public void registerObject(String bucketId, long ledgerId, long entryId, long lastEntryId, long size) throws ObjectManagerException;

    public void deleteObject(String bucketId, long ledgerId, long entryId) throws ObjectManagerException;

    public Collection<Long> listDeletableLedgers(String bucketId) throws ObjectManagerException;

    public Collection<LedgerMetadata> listLedgersbyBucketId(String bucketId) throws ObjectManagerException;

    public Collection<ObjectMetadata> listObjectsByLedger(String bucketId, long ledgerId) throws ObjectManagerException;

    public default void close() {
    }
}
