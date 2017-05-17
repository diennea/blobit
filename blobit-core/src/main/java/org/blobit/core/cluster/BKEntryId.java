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

/**
 * Coordinates on BookKeeper
 *
 * @author enrico.olivelli
 */
class BKEntryId {

    public final long ledgerId;
    public final long firstEntryId;
    public final long lastEntryId;

    public BKEntryId(long ledgerId, long firstEntryId, long lastEntryId) {
        this.ledgerId = ledgerId;
        this.firstEntryId = firstEntryId;
        this.lastEntryId = lastEntryId;
    }

    public String toId() {
        return ledgerId + "-" + firstEntryId + "-" + lastEntryId;
    }

    public static BKEntryId parseId(String id) {
        String[] split = id.split("-");
        return new BKEntryId(
            Long.parseLong(split[0]),
            Long.parseLong(split[1]),
            Long.parseLong(split[2]));
    }

}
