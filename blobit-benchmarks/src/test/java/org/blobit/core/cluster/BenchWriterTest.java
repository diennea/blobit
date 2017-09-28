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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
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
import org.blobit.core.cluster.ZKTestEnv;

public class BenchWriterTest {

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
    private static final int CONCURRENT_WRITERS = 10;

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
                    .setConcurrentWriters(CONCURRENT_WRITERS)
                    .setZookeeperUrl(env.getAddress());

            LongAdder totalTime = new LongAdder();
            try (ObjectManager blobManager = ObjectManagerFactory.createObjectManager(configuration, datasource);) {

                blobManager.createBucket(BUCKET_ID, BUCKET_ID, BucketConfiguration.DEFAULT).get();

                for (int j = 0; j < TEST_ITERATIONS; j++) {
                    long _start = System.currentTimeMillis();

                    Collection<Future<String>> batch = new ConcurrentLinkedQueue<>();
                    for (int i = 0; i < TEST_SIZE; i++) {
                        long _entrystart = System.currentTimeMillis();
                        CompletableFuture res = blobManager.put(BUCKET_ID, TEST_DATA).future;
                        res.handle((a, b) -> {
                            totalTime.add(System.currentTimeMillis() - _entrystart);
                            return a;
                        });
                        batch.add(res);
                    }

                    assertEquals(TEST_SIZE, batch.size());
                    List<String> ids = new ArrayList<>();
                    for (Future<String> f : batch) {
                        ids.add(f.get());
                    }
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
}
