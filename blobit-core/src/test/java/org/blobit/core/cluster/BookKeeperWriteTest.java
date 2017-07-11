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

import java.io.File;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.LongAdder;
import org.apache.bookkeeper.client.AsyncCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;
import static org.junit.Assert.assertEquals;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 *
 * @author enrico.olivelli
 */
public class BookKeeperWriteTest {

    private static final byte[] TEST_DATA = new byte[35 * 1024];
    private static final int testsize = 1000;
    private static final int clientwriters = 1;
    private static final int concurrentwriters = 1;
    @Rule
    public final TemporaryFolder tmp = new TemporaryFolder(new File("target").getAbsoluteFile());

    @Test
    public void test() throws Exception {
        try (ZKTestEnv env = new ZKTestEnv(tmp.newFolder("zk").toPath());) {
            env.startBookie();
            ClientConfiguration clientConfiguration = new ClientConfiguration();
            clientConfiguration.setThrottleValue(5000);
            clientConfiguration.setZkServers(env.getAddress());
            try (BookKeeper bk = new BookKeeper(clientConfiguration);) {
                for (int j = 0; j < 10; j++) {
                    try (LedgerHandle lh = bk.createLedger(1, 1, 1, BookKeeper.DigestType.CRC32, new byte[0])) {
                        LongAdder totalTime = new LongAdder();
                        long _start = System.currentTimeMillis();
                        Collection<CompletableFuture> batch = new ConcurrentLinkedQueue<>();
                        for (int i = 0; i < testsize; i++) {
                            CompletableFuture cf = new CompletableFuture();
                            batch.add(cf);
                            lh.asyncAddEntry(TEST_DATA, new AsyncCallback.AddCallback() {

                                long start = System.currentTimeMillis();

                                @Override
                                public void addComplete(int rc, LedgerHandle lh, long entryId, Object ctx) {
                                    long now = System.currentTimeMillis();
                                    CompletableFuture _cf = (CompletableFuture) ctx;
                                    if (rc == BKException.Code.OK) {
                                        _cf.complete("");
                                    } else {
                                        _cf.completeExceptionally(BKException.create(rc));
                                    }
                                    totalTime.add(now - start);
                                }
                            }, cf);
//                    Thread.sleep(1);
//                    Thread.yield();
                        }
                        assertEquals(testsize, batch.size());
                        for (CompletableFuture f : batch) {
                            f.get();
                        }
                        long _stop = System.currentTimeMillis();
                        long delta = _stop - _start;
                        System.out.println("Total time: " + delta + " ms");
                        System.out.println("#"+j+" Total real time: " + totalTime.sum() + " ms -> " + (totalTime.sum() / testsize) + " ms per entry");
                    }
                }
            }
        }
    }
}
