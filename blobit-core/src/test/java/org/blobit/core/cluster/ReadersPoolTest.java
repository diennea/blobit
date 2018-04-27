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
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import static org.apache.bookkeeper.common.concurrent.FutureUtils.result;
import org.apache.commons.pool2.impl.DefaultPooledObjectInfo;
import org.blobit.core.util.TestUtils;
import org.junit.Assert;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ReadersPoolTest {

    @Rule
    public final TemporaryFolder tmp = new TemporaryFolder();

    private static final String BUCKET_ID = "mybucket";
    private static final byte[] TEST_DATA = new byte[1 * 1024];

    static {
        Random random = new Random();
        random.nextBytes(TEST_DATA);
    }

    @Test
    public void testReaderNotUsingWriter() throws Exception {
        Properties dsProperties = new Properties();
        dsProperties.put(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_LOCAL);
        try (ZKTestEnv env = new ZKTestEnv(tmp.newFolder("zk").toPath());
                HerdDBEmbeddedDataSource datasource = new HerdDBEmbeddedDataSource(dsProperties)) {
            env.startBookie();
            Configuration configuration
                    = new Configuration()
                            .setType(Configuration.TYPE_BOOKKEEPER)
                            .setMaxBytesPerLedger(TEST_DATA.length - 1) // we want a new ledger for each blob
                            .setConcurrentReaders(2)
                            .setZookeeperUrl(env.getAddress());
            try (ClusterObjectManager manager = (ClusterObjectManager) ObjectManagerFactory.createObjectManager(configuration, datasource);
                    ClusterObjectManager managerReaders = (ClusterObjectManager) ObjectManagerFactory.createObjectManager(configuration, datasource);) {
                manager.createBucket(BUCKET_ID, BUCKET_ID, BucketConfiguration.DEFAULT).get();

                // perform a put, a new writer must be allocated
                String blobId = manager.put(BUCKET_ID, TEST_DATA).get();
                BKEntryId entryId = BKEntryId.parseId(blobId);

                BookKeeperBlobManager blobManagerReaders = managerReaders.getBlobManager();
                {
                    Map<String, List<DefaultPooledObjectInfo>> all = blobManagerReaders.readers.listAllObjects();
                    assertTrue(all.isEmpty());
                }

                managerReaders.get(BUCKET_ID, blobId).get();
                assertEquals(0, blobManagerReaders.getStats().getUsedWritersAsReaders());
                {
                    Map<String, List<DefaultPooledObjectInfo>> all = blobManagerReaders.readers.listAllObjects();
                    assertEquals(1, all.size());

                    DefaultPooledObjectInfo readerStats = all.get(entryId.ledgerId + "").get(0);
                    assertEquals(1, readerStats.getBorrowedCount());

                    managerReaders.get(BUCKET_ID, blobId).get();
                    assertEquals(2, readerStats.getBorrowedCount());
                }

                env.stopBookie();

                {
                    // get will fail
                    RuntimeException error = TestUtils.expectThrows(RuntimeException.class,
                            () -> FutureUtils.result(managerReaders.get(BUCKET_ID, blobId)));
                    assertTrue(error.getCause() instanceof BKException.BKBookieHandleNotAvailableException);

                    // reader is not evicted upon failures
                    Map<String, List<DefaultPooledObjectInfo>> all = blobManagerReaders.readers.listAllObjects();
                    assertEquals(1, all.size());
                }

                env.startBookie();
                Thread.sleep(2000);

                {
                    // get will now succeeed
                    managerReaders.get(BUCKET_ID, blobId).get();
                    Map<String, List<DefaultPooledObjectInfo>> all = blobManagerReaders.readers.listAllObjects();
                    assertEquals(1, all.size());
                    DefaultPooledObjectInfo readerStats = all.get(entryId.ledgerId + "").get(0);
                    assertEquals(1, readerStats.getBorrowedCount());
                }

                String blobId2 = manager.put(BUCKET_ID, TEST_DATA).get();
                BKEntryId entryId2 = BKEntryId.parseId(blobId2);
                Assert.assertNotEquals(entryId.ledgerId, entryId2.ledgerId);

                managerReaders.get(BUCKET_ID, blobId2).get();

                Map<String, List<DefaultPooledObjectInfo>> all = blobManagerReaders.readers.listAllObjects();
                assertEquals(2, all.size());
                DefaultPooledObjectInfo readerStats1 = all.get(entryId.ledgerId + "").get(0);
                assertEquals(1, readerStats1.getBorrowedCount());
                DefaultPooledObjectInfo readerStats2 = all.get(entryId2.ledgerId + "").get(0);
                assertEquals(1, readerStats2.getBorrowedCount());

                // third ledger, we have concurrentReaders = 2, so we can read concurrently only from 2 ledgers
                String blobId3 = manager.put(BUCKET_ID, TEST_DATA).get();
                BKEntryId entryId3 = BKEntryId.parseId(blobId3);
                Assert.assertNotEquals(entryId.ledgerId, entryId3.ledgerId);
                Assert.assertNotEquals(entryId2.ledgerId, entryId3.ledgerId);

                managerReaders.get(BUCKET_ID, blobId3).get();
                all = blobManagerReaders.readers.listAllObjects();
                assertEquals(2, all.size());

            }
        }
    }

    @Test
    public void testReaderUseOpenWriter() throws Exception {
        Properties dsProperties = new Properties();
        dsProperties.put(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_LOCAL);
        try (ZKTestEnv env = new ZKTestEnv(tmp.newFolder("zk").toPath());
                HerdDBEmbeddedDataSource datasource = new HerdDBEmbeddedDataSource(dsProperties)) {
            env.startBookie();
            Configuration configuration
                    = new Configuration()
                            .setType(Configuration.TYPE_BOOKKEEPER)
                            .setMaxBytesPerLedger(TEST_DATA.length) // we want a new ledger for each blob, but the first writer is not to be closed
                            .setConcurrentReaders(2)
                            .setZookeeperUrl(env.getAddress());
            try (ClusterObjectManager manager = (ClusterObjectManager) ObjectManagerFactory.createObjectManager(configuration, datasource);) {
                manager.createBucket(BUCKET_ID, BUCKET_ID, BucketConfiguration.DEFAULT).get();

                // perform a put, a new writer must be allocated
                String blobId = manager.put(BUCKET_ID, TEST_DATA).get();
                System.out.println("blobId:" + blobId);
                BKEntryId entryId = BKEntryId.parseId(blobId);

                BookKeeperBlobManager blobManager = manager.getBlobManager();
                {
                    Map<String, List<DefaultPooledObjectInfo>> all = blobManager.readers.listAllObjects();
                    assertTrue(all.isEmpty());
                }

                manager.get(BUCKET_ID, blobId).get();

                // ensure that we are using the LedgerHandle inside the Writer
                assertEquals(1, blobManager.getStats().getUsedWritersAsReaders());

                {
                    Map<String, List<DefaultPooledObjectInfo>> all = blobManager.readers.listAllObjects();
                    assertEquals(1, all.size());

                    DefaultPooledObjectInfo readerStats = all.get(entryId.ledgerId + "").get(0);
                    assertEquals(1, readerStats.getBorrowedCount());

                    manager.get(BUCKET_ID, blobId).get();
                    assertEquals(2, readerStats.getBorrowedCount());
                }

                env.stopBookie();

                {
                    Map<String, List<DefaultPooledObjectInfo>> allWriters = blobManager.writers.listAllObjects();
                    assertEquals(1, allWriters.size());

                    // get will fail
                    RuntimeException error = TestUtils.expectThrows(RuntimeException.class,
                            () -> FutureUtils.result(manager.get(BUCKET_ID, blobId)));
                    assertTrue(error.getCause() instanceof BKException.BKBookieHandleNotAvailableException);

                    assertEquals(1, blobManager.getStats().getUsedWritersAsReaders());

                    // writers pool is not touched
                    allWriters = blobManager.writers.listAllObjects();
                    assertEquals(1, allWriters.size());

                    // reader is not evicted upon failures
                    Map<String, List<DefaultPooledObjectInfo>> allReaders = blobManager.readers.listAllObjects();
                    assertEquals(1, allReaders.size());

                }

                {
                    // put will fail, writer will be eventually disposed
                    RuntimeException error = TestUtils.expectThrows(RuntimeException.class,
                            () -> FutureUtils.result(manager.put(BUCKET_ID, TEST_DATA).future));
                    assertTrue(error.getCause() instanceof BKException.BKNotEnoughBookiesException);
                    Map<String, List<DefaultPooledObjectInfo>> allWriters = blobManager.writers.listAllObjects();
                    assertTrue(allWriters.isEmpty());
                }

                env.startBookie();
                Thread.sleep(2000);

                {
                    // get will now succeeed
                    manager.get(BUCKET_ID, blobId).get();
                    assertEquals(1, blobManager.getStats().getUsedWritersAsReaders());
                    Map<String, List<DefaultPooledObjectInfo>> allReaders = blobManager.readers.listAllObjects();
                    assertEquals(1, allReaders.size());
                    DefaultPooledObjectInfo readerStats = allReaders.get(entryId.ledgerId + "").get(0);
                    assertEquals(1, readerStats.getBorrowedCount());
                }

                String blobId2 = manager.put(BUCKET_ID, TEST_DATA).get();                
                BKEntryId entryId2 = BKEntryId.parseId(blobId2);
                Assert.assertNotEquals(entryId.ledgerId, entryId2.ledgerId);

                manager.get(BUCKET_ID, blobId2).get();

                Map<String, List<DefaultPooledObjectInfo>> all = blobManager.readers.listAllObjects();
                assertEquals(2, all.size());
                DefaultPooledObjectInfo readerStats1 = all.get(entryId.ledgerId + "").get(0);
                assertEquals(1, readerStats1.getBorrowedCount());
                DefaultPooledObjectInfo readerStats2 = all.get(entryId2.ledgerId + "").get(0);
                assertEquals(1, readerStats2.getBorrowedCount());

                // third ledger, we have concurrentReaders = 2, so we can read concurrently only from 2 ledgers
                // we need to create a new ledger
                manager.put(BUCKET_ID, TEST_DATA).get();
                manager.put(BUCKET_ID, TEST_DATA).get();
                manager.put(BUCKET_ID, TEST_DATA).get();
                manager.put(BUCKET_ID, TEST_DATA).get();

                String blobId3 = manager.put(BUCKET_ID, TEST_DATA).get();                
                BKEntryId entryId3 = BKEntryId.parseId(blobId3);
                Assert.assertNotEquals(entryId.ledgerId, entryId3.ledgerId);
                Assert.assertNotEquals(entryId2.ledgerId, entryId3.ledgerId);

                manager.get(BUCKET_ID, blobId3).get();
                all = blobManager.readers.listAllObjects();
                System.out.println("all keys:" + all.keySet());
                assertEquals(2, all.size());

            }
        }
    }
}
