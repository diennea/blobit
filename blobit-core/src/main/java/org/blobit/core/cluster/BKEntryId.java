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
    public final int entrySize;
    public final long length;
    public final int numEntries;

    public static final String EMPTY_ENTRY_ID = formatId(0, 0, 0, 0, 0);

    public BKEntryId(long ledgerId, long firstEntryId, int entrySize,
            long length, int numEntries) {
        this.ledgerId = ledgerId;
        this.firstEntryId = firstEntryId;
        this.entrySize = entrySize;
        this.length = length;
        this.numEntries = numEntries;
    }

    public String toId() {
        return formatId(ledgerId, firstEntryId, entrySize, length, numEntries);
    }

    static String formatId(long ledgerId, long firstEntryId, int entrySize,
            long length, int numEntries) {
        StringBuilder res = new StringBuilder();
        res.append(ledgerId);
        res.append('-');
        res.append(firstEntryId);
        res.append('-');
        res.append(entrySize);
        res.append('-');
        res.append(length);
        res.append('-');
        res.append(numEntries);
        return res.toString();
    }

    public static BKEntryId parseId(String id) {
        String[] split = id.split("-");
        return new BKEntryId(
                Long.parseLong(split[0]),
                Long.parseLong(split[1]),
                Integer.parseInt(split[2]),
                Long.parseLong(split[3]),
                Integer.parseInt(split[4])
        );
    }

    @Override
    public String toString() {
        return "BKEntryId{" + "ledgerId=" + ledgerId + ", firstEntryId=" + firstEntryId + ", entrySize=" + entrySize
                + ", length=" + length + ", numEntries=" + numEntries + '}';
    }

}
