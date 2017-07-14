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

import herddb.jdbc.HerdDBEmbeddedDataSource;
import herddb.server.ServerConfiguration;
import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.logging.LogManager;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.blobit.core.api.ObjectManagerFactory;
import org.blobit.core.api.BucketConfiguration;
import org.blobit.core.api.Configuration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.blobit.core.api.ObjectManager;
import static org.junit.Assert.assertEquals;

public class BenchWriterSynchClientsTest {

    static {
        LogManager.getLogManager().reset();
        Logger.getRootLogger().setLevel(Level.INFO);
    }

    @Rule
    public final TemporaryFolder tmp = new TemporaryFolder(new File("target").getAbsoluteFile());

    private static final String BUCKET_ID = "mybucket";
    private static final byte[] TEST_DATA = new byte[35 * 1024];
    private static final int TESTSIZE = 1000;
    private static final int clientwriters = 10;
    private static final int concurrentwriters = 10;

    static {
        Random random = new Random();
        random.nextBytes(TEST_DATA);
    }

    @Test
    public void testWrite() throws Exception {
        Properties dsProperties = new Properties();
        dsProperties.put(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_LOCAL);
        try (ZKTestEnv env = new ZKTestEnv(tmp.newFolder("zk").toPath());
            HerdDBEmbeddedDataSource datasource = new HerdDBEmbeddedDataSource(dsProperties)) {
            env.startBookie();
            Configuration configuration
                = new Configuration()
                    .setType(Configuration.TYPE_BOOKKEEPER)
                    .setConcurrentWriters(concurrentwriters)
                    .setZookeeperUrl(env.getAddress());

            LongAdder totalTime = new LongAdder();
            try (ObjectManager blobManager = ObjectManagerFactory.createObjectManager(configuration, datasource);) {

                for (int i = 0; i < clientwriters; i++) {
                    blobManager.getMetadataStorageManager().createBucket(BUCKET_ID + i, BUCKET_ID + i, BucketConfiguration.DEFAULT);
                }

                for (int j = 0; j < 1000; j++) {
                    AtomicInteger totalDone = new AtomicInteger();
                    long _start = System.currentTimeMillis();
                    Map<String, AtomicInteger> numMessagesPerClient = new ConcurrentHashMap<>();

                    Thread[] clients = new Thread[clientwriters];
                    for (int tname = 0; tname < clients.length; tname++) {
                        final String name = "client-" + tname;
                        final int _tname = tname;
                        final AtomicInteger counter = new AtomicInteger();
                        numMessagesPerClient.put(name, counter);
                        Thread tr = new Thread(new Runnable() {
                            @Override
                            public void run() {
                                try {
                                    for (int i = 0; i < TESTSIZE / clientwriters; i++) {
                                        writeData(_tname, blobManager, counter, totalTime);
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
                        assertEquals("bad count for " + entry.getKey(), TESTSIZE / clientwriters, entry.getValue().get());
                    }
                    assertEquals(TESTSIZE, totalDone.get());

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

    private void writeData(int tname, ObjectManager blobManager, AtomicInteger counter, LongAdder totalTime) throws InterruptedException, ExecutionException {
        String tName = Thread.currentThread().getName();
        long _entrystart = System.currentTimeMillis();
        CompletableFuture<String> res = blobManager.put(BUCKET_ID + tname, TEST_DATA);
        res.handle((a, b) -> {
            long time = System.currentTimeMillis() - _entrystart;
            totalTime.add(time);

            int actualCount = counter.incrementAndGet();
//            System.out.println("client " + tName + " wrote entry " + a + " on " + BUCKET_ID + tname + "," + time + " ms #" + actualCount+", "+b);

            return a;
        }).get();
    }
}
