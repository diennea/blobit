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

import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.blobit.core.api.BucketConfiguration;
import org.blobit.core.api.Configuration;
import org.blobit.core.api.ObjectManagerFactory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import herddb.jdbc.HerdDBEmbeddedDataSource;
import herddb.server.ServerConfiguration;
import java.util.Map;
import org.apache.bookkeeper.client.BKException;
import org.apache.commons.pool2.impl.DefaultPooledObjectInfo;
import org.blobit.core.api.BucketHandle;
import org.blobit.core.api.ObjectManagerException;
import org.blobit.core.util.TestUtils;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class WritersPoolTest {

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
                            .setMaxBytesPerLedger(TEST_DATA.length * 2 - 1)
                            .setZookeeperUrl(env.getAddress());
            try (ClusterObjectManager manager = (ClusterObjectManager) ObjectManagerFactory.createObjectManager(configuration, datasource);) {
                manager.createBucket(BUCKET_ID, BUCKET_ID, BucketConfiguration.DEFAULT).get();
                BucketHandle bucket = manager.getBucket(BUCKET_ID);
                // perform a put, a new writer must be allocated
                bucket.put(TEST_DATA).get();

                BookKeeperBlobManager blobManager = manager.getBlobManager();
                {
                    Map<String, List<DefaultPooledObjectInfo>> all = blobManager.writers.listAllObjects();
                    List<DefaultPooledObjectInfo> writers = all.get(BUCKET_ID);
                    assertEquals(1, writers.size());
                    DefaultPooledObjectInfo writerStats = writers.get(0);
                    System.out.println("Stats: " + writerStats.getBorrowedCount());
                    assertEquals(1, writerStats.getBorrowedCount());

                    // new put, same writer
                    bucket.put(TEST_DATA).get();
                    assertEquals(2, writerStats.getBorrowedCount());
                }

                {
                    // we have passed MaxBytesPerLedger, writer is no more valid, so it is disposed
                    Map<String, List<DefaultPooledObjectInfo>> all = blobManager.writers.listAllObjects();
                    assertTrue(all.isEmpty());
                }

                {
                    // new put, new writer, we have passed MaxBytesPerLedger
                    bucket.put(TEST_DATA).get();
                    Map<String, List<DefaultPooledObjectInfo>> all = blobManager.writers.listAllObjects();
                    List<DefaultPooledObjectInfo> writers = all.get(BUCKET_ID);
                    assertEquals(1, writers.size());
                    DefaultPooledObjectInfo writerStats = writers.get(0);
                    System.out.println("Stats: " + writerStats.getBorrowedCount());
                    assertEquals(1, writerStats.getBorrowedCount());
                }

                env.stopBookie();

                {
                    // put will fail, writer will be eventually disposed
                    ObjectManagerException error = TestUtils.expectThrows(ObjectManagerException.class,
                            () -> bucket.put(TEST_DATA).get());
                    assertTrue(error.getCause() instanceof BKException.BKNotEnoughBookiesException);
                    Map<String, List<DefaultPooledObjectInfo>> all = blobManager.writers.listAllObjects();
                    assertTrue(all.isEmpty());
                }

                env.startBookie();
                Thread.sleep(2000);

                {
                    // put will succeeed
                    bucket.put(TEST_DATA).get();
                    Map<String, List<DefaultPooledObjectInfo>> all = blobManager.writers.listAllObjects();
                    assertEquals(1, all.size());
                }

            }
        }
    }
}
