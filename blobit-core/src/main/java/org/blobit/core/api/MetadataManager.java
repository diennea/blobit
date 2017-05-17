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
        BucketConfiguration configuration) throws DataManagerException;

    public void init() throws DataManagerException;

    public Collection<BucketMetadata> listBuckets() throws DataManagerException;

    public void registerLedger(String bucketId, long ledgerId) throws DataManagerException;

    public void deleteLedger(String bucketId, long ledgerId) throws DataManagerException;

    public void registerObject(String bucketId, long ledgerId, long entryId, long lastEntryId, long size) throws DataManagerException;

    public void deleteObject(String bucketId, long ledgerId, long entryId) throws DataManagerException;

    public Collection<Long> listDeletableLedgers(String bucketId) throws DataManagerException;

    public Collection<LedgerMetadata> listLedgersbyBucketId(String bucketId) throws DataManagerException;

    public Collection<ObjectMetadata> listBlobsByLedger(String bucketId, long ledgerId) throws DataManagerException;

    public default void close() {
    }
}
