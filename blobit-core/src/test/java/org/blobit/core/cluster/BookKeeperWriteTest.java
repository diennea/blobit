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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.File;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.LongAdder;
import java.util.logging.LogManager;
import org.apache.bookkeeper.client.AsyncCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import static org.junit.Assert.assertEquals;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 *
 * @author enrico.olivelli
 */
public class BookKeeperWriteTest {

    static {
        LogManager.getLogManager().reset();
        Logger.getRootLogger().setLevel(Level.INFO);
    }

    private static final byte[] TEST_DATA = new byte[35*1024];
    private static final int TESTSIZE = 1000;

    @Rule
    public final TemporaryFolder tmp = new TemporaryFolder(new File("target").getAbsoluteFile());

    @Test
    public void test() throws Exception {
        try (ZKTestEnv env = new ZKTestEnv(tmp.newFolder("zk").toPath());) {
            env.startBookie();
            ClientConfiguration clientConfiguration = new ClientConfiguration();

            // this is like (sleep(1)) in the loop below
            clientConfiguration.setThrottleValue(0);

            clientConfiguration.setZkServers(env.getAddress());

            // this reduce most of GC
            clientConfiguration.setUseV2WireProtocol(true);

            ByteBuf data = Unpooled.directBuffer(TEST_DATA.length);
            data.writeBytes(TEST_DATA);

            try (BookKeeper bk = new BookKeeper(clientConfiguration);) {

                for (int j = 0; j < 10; j++) {
                    try (
                        LedgerHandle lh = bk.createLedger(1, 1, 1, BookKeeper.DigestType.CRC32, new byte[0])) {
                        LongAdder totalTime = new LongAdder();
                        long _start = System.currentTimeMillis();
                        Collection<CompletableFuture> batch = new ConcurrentLinkedQueue<>();
                        for (int i = 0; i < TESTSIZE; i++) {
                            CompletableFuture cf = new CompletableFuture();
                            batch.add(cf);
                            lh.asyncAddEntry(data, new AsyncCallback.AddCallback() {

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

//                          Thread.sleep(1);
                        }
                        assertEquals(TESTSIZE, batch.size());
                        for (CompletableFuture f : batch) {
                            f.get();
                        }
                        long _stop = System.currentTimeMillis();
                        double delta = _stop - _start;
                        System.out.printf("#" + j + " Wall clock time: " + delta + " ms, "
                            // + "total callbacks time: " + totalTime.sum() + " ms, "
                            + "size %.3f MB -> %.2f ms per entry (latency),"
                            + "%.1f ms per entry (throughput) %.1f MB/s throughput%n",
                            (TEST_DATA.length / (1024 * 1024d)),
                            (totalTime.sum() * 1d / TESTSIZE),
                            (delta / TESTSIZE),
                            ((((TESTSIZE * TEST_DATA.length) / (1024 * 1024d))) / (delta / 1000d)));
                    }
                }

            }
        }
    }
}
