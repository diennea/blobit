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
import static org.junit.Assert.fail;

import java.io.File;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.logging.LogManager;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.blobit.core.api.BucketConfiguration;
import org.blobit.core.api.Configuration;
import org.blobit.core.api.ObjectManager;
import org.blobit.core.api.ObjectManagerFactory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import herddb.jdbc.HerdDBEmbeddedDataSource;
import herddb.server.ServerConfiguration;

public class BenchWriterSynchClientsTest {

    static {
        LogManager.getLogManager().reset();
        Logger.getRootLogger().setLevel(Level.INFO);
    }

    @Rule
    public final TemporaryFolder tmp = new TemporaryFolder(new File("target").getAbsoluteFile());

    private static final String BUCKET_ID = "mybucket";
    private static final byte[] TEST_DATA = new byte[35 * 1024];
    private static final int TEST_SIZE = 1000;
    private static final int TEST_ITERATIONS = 10;
    private static final int CLIENT_WRITERS = 10;
    private static final int CONCURRENT_WRITERS = 10;

    static {
        Random random = new Random();
        random.nextBytes(TEST_DATA);
    }

    @Test
    public void testWrite() throws Exception {
        AtomicReference<Throwable> error = new AtomicReference<>();
        Properties dsProperties = new Properties();
        dsProperties.put(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_LOCAL);
        try (ZKTestEnv env = new ZKTestEnv(tmp.newFolder("zk").toPath());
            HerdDBEmbeddedDataSource datasource = new HerdDBEmbeddedDataSource(dsProperties)) {
            env.startBookie();
            Configuration configuration
                = new Configuration()
                    .setType(Configuration.TYPE_BOOKKEEPER)
                    .setConcurrentWriters(CONCURRENT_WRITERS)
                    .setZookeeperUrl(env.getAddress());

            try (ObjectManager blobManager = ObjectManagerFactory.createObjectManager(configuration, datasource);) {

                for (int i = 0; i < CLIENT_WRITERS; i++) {
                    blobManager.createBucket(BUCKET_ID + i, BUCKET_ID + i, BucketConfiguration.DEFAULT);
                }

                for (int j = 0; j < TEST_ITERATIONS; j++) {
                    LongAdder totalTime = new LongAdder();
                    AtomicInteger totalDone = new AtomicInteger();
                    long _start = System.currentTimeMillis();
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
                                try {
                                    for (int i = 0; i < TEST_SIZE / CLIENT_WRITERS; i++) {
                                        writeData(_tname, blobManager, counter, totalTime);
                                        totalDone.incrementAndGet();
                                    }
                                } catch (Throwable t) {
                                    fail("error " + t);
                                    error.set(t);
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

                    if (error.get() != null) {
                        fail(error.get().toString());
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

    private void writeData(int tname, ObjectManager blobManager, AtomicInteger counter, LongAdder totalTime) throws InterruptedException, ExecutionException {
        String tName = Thread.currentThread().getName();
        long _entrystart = System.currentTimeMillis();
        CompletableFuture<?> res = blobManager.put(BUCKET_ID + tname, TEST_DATA).future;
        res.handle((a, b) -> {
            if (b != null) {
                throw new RuntimeException(b);
            }
            long time = System.currentTimeMillis() - _entrystart;
            totalTime.add(time);

            int actualCount = counter.incrementAndGet();
//            System.out.println("client " + tName + " wrote entry " + a + " on " + BUCKET_ID + tname + "," + time + " ms #" + actualCount+", "+b);

            return a;
        }).get();
    }
}
