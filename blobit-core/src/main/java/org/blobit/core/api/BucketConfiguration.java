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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;

public final class BucketConfiguration {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static final BucketConfiguration DEFAULT = new BucketConfiguration();

    private int replicaCount = Configuration.REPLICATION_FACTOR_DEFAULT;
    private long metadataLeaderInactivityTime = Configuration.LEADER_INACTIVITY_TIME_DEFAULT;

    public int getReplicaCount() {
        return replicaCount;
    }

    public void setReplicaCount(int replicaCount) {
        this.replicaCount = replicaCount;
    }

    public long getMetadataLeaderInactivityTime() {
        return metadataLeaderInactivityTime;
    }

    public void setMetadataLeaderInactivityTime(long metadataLeaderInactivityTime) {
        this.metadataLeaderInactivityTime = metadataLeaderInactivityTime;
    }

    public String serialize() {
        try {
            return MAPPER.writeValueAsString(this);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public static BucketConfiguration deserialize(String s) {
        if (s == null) {
            return new BucketConfiguration();
        } else {
            try {
                return MAPPER.readValue(s, BucketConfiguration.class);
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

}
