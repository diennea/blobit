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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.blobit.core.api.LocationInfo;

/**
 * Implementation of LocationInfo
 *
 * @author eolivelli
 */
public class BKLocationInfo implements LocationInfo {

    private final BKEntryId bk;
    private final LedgerMetadata metadata;

    BKLocationInfo(BKEntryId bk, LedgerMetadata value) {
        this.bk = bk;
        this.metadata = value;
    }

    @Override
    public String getId() {
        return bk.toId();
    }

    @Override
    public List<ServerInfo> getServersAtPosition(long offset) {
        if (offset < 0 || offset >= bk.length) {
            return Collections.emptyList();
        }
        long entryNum = (offset + 1) / bk.entrySize;
        List<BookieSocketAddress> ensembleAt = metadata.getEnsembleAt(entryNum);
        return ensembleAt
                .stream()
                .map(ba -> new BKServerInfo(ba.getHostName() + ":" + ba.getPort()))
                .collect(Collectors.toList());
    }

    @Override
    public long getSize() {
        return bk.length;
    }

    @Override
    public List<Long> getSegmentsStartOffsets() {
        if (bk.length == 0) {
            return Collections.emptyList();
        }
        List<Long> res = new ArrayList<>();
        long pos = 0;
        while (pos < bk.length) {
            res.add(pos);
            pos += bk.entrySize;
        }
        return res;
    }

    public static final class BKServerInfo implements ServerInfo {

        private final String address;

        private BKServerInfo(String server) {
            this.address = server;
        }

        @Override
        public String getAddress() {
            return address;
        }

        @Override
        public int hashCode() {
            int hash = 3;
            hash = 41 * hash + Objects.hashCode(this.address);
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
            final BKServerInfo other = (BKServerInfo) obj;
            if (!Objects.equals(this.address, other.address)) {
                return false;
            }
            return true;
        }

    }

}
