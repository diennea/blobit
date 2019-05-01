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

import org.blobit.core.api.ObjectManagerException;

/**
 * Coordinates on Memory
 *
 * @author enrico.olivelli
 */
class MemEntryId {

    public final long ledgerId;
    public final long entryId;
    public final long length;

    public MemEntryId(long ledgerId, long id, long size) {
        this.ledgerId = ledgerId;
        this.entryId = id;
        this.length = size;
    }

    public String toId() {
        return ledgerId + "-" + entryId + "-" + length;
    }

    public static MemEntryId parseId(String id) throws ObjectManagerException {
        try {
            String[] split = id.split("-");
            return new MemEntryId(
                    Long.parseLong(split[0]),
                    Long.parseLong(split[1]),
                    Long.parseLong(split[2]));
        } catch (NumberFormatException | ArrayIndexOutOfBoundsException err) {
            throw new ObjectManagerException(err);
        }
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 53 * hash + (int) (this.ledgerId ^ (this.ledgerId >>> 32));
        hash = 53 * hash + (int) (this.entryId ^ (this.entryId >>> 32));
        hash = 53 * hash + (int) (this.length ^ (this.length >>> 32));
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
        if (this.entryId != other.entryId) {
            return false;
        }
        if (this.length != other.length) {
            return false;
        }
        return true;
    }

}
