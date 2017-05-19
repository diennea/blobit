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

/**
 * Coordinates on Memory
 *
 * @author enrico.olivelli
 */
class MemEntryId {

    public final long ledgerId;
    public final long firstEntryId;
    public final long lastEntryId;

    public MemEntryId(long ledgerId, long firstEntryId, long lastEntryId) {
        this.ledgerId = ledgerId;
        this.firstEntryId = firstEntryId;
        this.lastEntryId = lastEntryId;
    }

    public String toId() {
        return ledgerId + "-" + firstEntryId + "-" + lastEntryId;
    }

    public static MemEntryId parseId(String id) {
        String[] split = id.split("-");
        return new MemEntryId(
            Long.parseLong(split[0]),
            Long.parseLong(split[1]),
            Long.parseLong(split[2]));
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 53 * hash + (int) (this.ledgerId ^ (this.ledgerId >>> 32));
        hash = 53 * hash + (int) (this.firstEntryId ^ (this.firstEntryId >>> 32));
        hash = 53 * hash + (int) (this.lastEntryId ^ (this.lastEntryId >>> 32));
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final MemEntryId other = (MemEntryId) obj;
        if (this.ledgerId != other.ledgerId) {
            return false;
        }
        if (this.firstEntryId != other.firstEntryId) {
            return false;
        }
        if (this.lastEntryId != other.lastEntryId) {
            return false;
        }
        return true;
    }



}
