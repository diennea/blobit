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
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Future;
import org.blobit.core.api.ObjectManagerFactory;
import org.blobit.core.api.BucketConfiguration;
import org.blobit.core.api.Configuration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.blobit.core.api.ObjectManager;

public class SimpleClusterWriterTest {

//    @Before
//    public void setupLogger() throws Exception {
//        Level level = Level.INFO;
//        Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
//
//            @Override
//            public void uncaughtException(Thread t, Throwable e) {
//                System.err.println("uncaughtException from thread " + t.getName() + ": " + e);
//                e.printStackTrace();
//            }
//        });
//        java.util.logging.LogManager.getLogManager().reset();
//        ConsoleHandler ch = new ConsoleHandler();
//        ch.setLevel(level);
//        SimpleFormatter f = new SimpleFormatter();
//        ch.setFormatter(f);
//        java.util.logging.Logger.getLogger("").setLevel(level);
//        java.util.logging.Logger.getLogger("").addHandler(ch);
//    }
    @Rule
    public final TemporaryFolder tmp = new TemporaryFolder();

    private static final String BUCKET_ID = "mybucket";
    private static final byte[] TEST_DATA = new byte[1 * 1024];

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
                    .setConcurrentWriters(4)
                    .setZookeeperUrl(env.getAddress());
            try (ObjectManager blobManager = ObjectManagerFactory.createObjectManager(configuration, datasource);) {
                long _start = System.currentTimeMillis();

                blobManager.getMetadataStorageManager().createBucket(BUCKET_ID, BUCKET_ID, BucketConfiguration.DEFAULT);

                blobManager.put(BUCKET_ID, TEST_DATA).get();
                
                List<Future<String>> batch = new ArrayList<>();
                for (int i = 0; i < 1000; i++) {
                    batch.add(blobManager.put(BUCKET_ID, TEST_DATA));
                }
                List<String> ids = new ArrayList<>();
                for (Future<String> f : batch) {
                    ids.add(f.get());
                }
                for (String id : ids) {
                    blobManager.delete(BUCKET_ID, id).get();
                }

                long _stop = System.currentTimeMillis();
                double speed = (int) (batch.size() * 60_000.0 / (_stop - _start));
                double band = speed * TEST_DATA.length;
                long total = (batch.size() * TEST_DATA.length * 1L) / (1024 * 1024);
                System.out.println("TIME: " + (_stop - _start) + " ms for " + batch.size() + " blobs, total " + total + " MBs, " + speed + " blobs/h " + (band / 1e9) + " Gbytes/h");
            }
        }
    }
}
