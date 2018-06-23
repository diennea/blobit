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

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.logging.LogManager;

import org.apache.bookkeeper.client.AsyncCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.blobit.core.cluster.ZKTestEnv;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 *
 * @author enrico.olivelli
 */
public class BookKeeperWriteSynchClientsTest {

    private static final byte[] TEST_DATA = new byte[35 * 1024];
    private static final int TEST_SIZE = 1000;
    private static final int TEST_ITERATIONS = 10;
    private static final int CLIENT_WRITERS = 10;

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

            try (BookKeeper bk = new BookKeeper(clientConfiguration);) {

                for (int j = 0; j < TEST_ITERATIONS; j++) {

                    LongAdder totalTime = new LongAdder();
                    long _start = System.currentTimeMillis();

                    AtomicInteger totalDone = new AtomicInteger();

                    Map<String, AtomicInteger> numMessagesPerClient = new ConcurrentHashMap<>();

                    Thread[] clients = new Thread[CLIENT_WRITERS];
                    for (int tname = 0; tname < clients.length; tname++) {
                        final String name = "client-" + tname;
                        final int _tname = tname;
                        final AtomicInteger counter = new AtomicInteger();
                        numMessagesPerClient.put(name, counter);
                        Thread tr = new Thread(new Runnable() {
                            @Override
                            public void run() {
                                try (
                                    LedgerHandle lh = bk.createLedger(1, 1, 1, BookKeeper.DigestType.CRC32, new byte[0])) {
                                    for (int i = 0; i < TEST_SIZE / CLIENT_WRITERS; i++) {
                                        writeData(_tname, lh, counter, totalTime);
                                        totalDone.incrementAndGet();
                                    }
                                } catch (Throwable t) {
                                    t.printStackTrace();
                                }
                            }
                        }, name);
                        clients[tname] = tr;
                    }
                    for (Thread t : clients) {
                        t.start();
                    }

                    for (Thread t : clients) {
                        t.join();
                    }

                    for (Map.Entry<String, AtomicInteger> entry : numMessagesPerClient.entrySet()) {
                        assertEquals("bad count for " + entry.getKey(), TEST_SIZE / CLIENT_WRITERS, entry.getValue().get());
                    }
                    assertEquals(TEST_SIZE, totalDone.get());

                    long _stop = System.currentTimeMillis();
                    double delta = _stop - _start;
                    System.out.printf("#" + j + " Wall clock time: " + delta + " ms, "
                        // + "total callbacks time: " + totalTime.sum() + " ms, "
                        + "size %.3f MB -> %.2f ms per entry (latency),"
                        + "%.1f ms per entry (throughput) %.1f MB/s throughput%n",
                        (TEST_DATA.length / (1024 * 1024d)),
                        (totalTime.sum() * 1d / TEST_SIZE),
                        (delta / TEST_SIZE),
                        ((((TEST_SIZE * TEST_DATA.length) / (1024 * 1024d))) / (delta / 1000d)));

                }
            }

        }
    }

    private void writeData(int tname, LedgerHandle lh, AtomicInteger counter, LongAdder totalTime) throws InterruptedException, ExecutionException {
        CompletableFuture cf = new CompletableFuture();
        lh.asyncAddEntry(TEST_DATA, new AsyncCallback.AddCallback() {

            long start = System.currentTimeMillis();

            @Override
            public void addComplete(int rc, LedgerHandle lh, long entryId, Object ctx) {
                int actualCount = counter.incrementAndGet();
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
        cf.get();

    }
}
