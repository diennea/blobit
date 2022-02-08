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

public class BucketMetadata {

    private final String bucketId;
    private final BucketConfiguration configuration;
    private final String tableSpaceName;
    private final String uuid;
    private final int status;

    public static final int STATUS_MARKED_FOR_DELETION = 1;
    public static final int STATUS_ACTIVE = 0;

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings(
            value = "EI_EXPOSE_REP2")
    public BucketMetadata(String bucketId,
            String uuid,
            int status,
            BucketConfiguration configuration,
            String tableSpaceName) {
        this.bucketId = bucketId;
        this.configuration = configuration;
        this.tableSpaceName = tableSpaceName;
        this.uuid = uuid;
        this.status = status;
    }

    public int getStatus() {
        return status;
    }

    public String getUuid() {
        return uuid;
    }

    public String getBucketId() {
        return bucketId;
    }

    @edu.umd.cs.findbugs.annotations.SuppressFBWarnings(
            value = "EI_EXPOSE_REP")
    public BucketConfiguration getConfiguration() {
        return configuration;
    }

    public String getTableSpaceName() {
        return tableSpaceName;
    }

}
