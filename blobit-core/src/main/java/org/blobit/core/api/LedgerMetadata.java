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

/**
 * Metadata about a ledger
 *
 * @author enrico.olivelli
 */
public class LedgerMetadata {

    private final long id;
    private final String bucketId;

    public LedgerMetadata(String bucketId, long id) {
        this.id = id;
        this.bucketId = bucketId;
    }

    public long getId() {
        return id;
    }

    public String getBucketId() {
        return bucketId;
    }

    @Override
    public String toString() {
        return "LedgerMetadata{" + "id=" + id + ", bucketId=" + bucketId + '}';
    }

}
