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
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.blobit.core.api.BucketConfiguration;
import org.blobit.core.api.Configuration;
import org.blobit.core.api.ObjectManager;
import org.blobit.core.api.ObjectManagerFactory;
import org.blobit.core.api.PutPromise;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import herddb.jdbc.HerdDBEmbeddedDataSource;
import herddb.server.ServerConfiguration;
import java.util.concurrent.CompletableFuture;
import org.blobit.core.api.BucketHandle;
import org.blobit.core.api.GetPromise;

public class SimpleClusterReadWriteTest {

    @Rule
    public final TemporaryFolder tmp = new TemporaryFolder(new File("target").getAbsoluteFile());

    private static final String BUCKET_ID = "mybucket";
    private static final byte[] TEST_DATA = new byte[100 * 1024];

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
                            .setConcurrentWriters(10)
                            .setZookeeperUrl(env.getAddress());

            try (ObjectManager manager = ObjectManagerFactory.createObjectManager(configuration, datasource);) {
                long _start = System.currentTimeMillis();
                manager.createBucket(BUCKET_ID, BUCKET_ID, BucketConfiguration.DEFAULT).get();
                List<PutPromise> batch = new ArrayList<>();
                BucketHandle bucket = manager.getBucket(BUCKET_ID);
                for (int i = 0; i < 1000; i++) {
                    batch.add(bucket.put(null, TEST_DATA));

//                    Thread.sleep(1);
                }
                List<String> ids = new ArrayList<>();
                for (PutPromise f : batch) {
                    ids.add(f.get());
                }

                long _stopWrite = System.currentTimeMillis();
                double speedWrite = (int) (batch.size() * 60_000.0 / (_stopWrite - _start));
                double bandWrite = speedWrite * TEST_DATA.length;

                long total = (batch.size() * TEST_DATA.length * 1L) / (1024 * 1024);
                System.out.println("TIME: " + (_stopWrite - _start) + " ms for " + batch.size() + " blobs, total " + total + " MBs, " + speedWrite + " blobs/h, " + speedWrite * 24 + " blobs/day " + (bandWrite / 1e9) + " Gbytes/h");

                _start = _stopWrite;

                List<GetPromise> read = new ArrayList<>();
                for (String id : ids) {
//                    System.out.println("waiting for id " + id);
                    read.add(bucket.get(id));

                }
                for (GetPromise get : read) {
                    Assert.assertArrayEquals(TEST_DATA, get.get());
                }

                long _stop = System.currentTimeMillis();

                double speed = (int) (batch.size() * 60_000.0 / (_stop - _start));
                double band = speed * TEST_DATA.length;
                System.out.println("TIME: " + (_stop - _start) + " ms for " + batch.size() + " blobs, total " + total + " MBs, " + speed + " blobs/h, " + speed * 24 + " blobs/day " + (band / 1e9) + " Gbytes/h");
            }
        }
    }
}
