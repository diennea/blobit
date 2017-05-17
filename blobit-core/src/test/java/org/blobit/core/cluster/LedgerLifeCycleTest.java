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
import java.util.Collection;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import org.blobit.core.api.ObjectManagerFactory;
import org.blobit.core.api.ObjectMetadata;
import org.blobit.core.api.BucketConfiguration;
import org.blobit.core.api.Configuration;
import org.blobit.core.api.LedgerMetadata;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.blobit.core.api.MetadataManager;
import org.blobit.core.api.ObjectManager;

public class LedgerLifeCycleTest {

    @Rule
    public final TemporaryFolder tmp = new TemporaryFolder();

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
            try (ObjectManager blobManager = ObjectManagerFactory.createObjectManager(configuration, datasource);) {
                long _start = System.currentTimeMillis();

                MetadataManager metadataManager = blobManager.getMetadataStorageManager();

                try {
                    blobManager.put(BUCKET_ID, TEST_DATA).get();
                    fail();
                } catch (ExecutionException ok) {
                    ok.printStackTrace();
                }

                metadataManager.createBucket(BUCKET_ID, BUCKET_ID, BucketConfiguration.DEFAULT);
                String id = blobManager.put(BUCKET_ID, TEST_DATA).get();

                {
                    Collection<LedgerMetadata> ledgers = metadataManager.listLedgersbyBucketId(BUCKET_ID);
                    for (LedgerMetadata l : ledgers) {
//                        System.out.println("LedgerMetadata:" + l);
                        Collection<ObjectMetadata> blobs = metadataManager.listBlobsByLedger(BUCKET_ID, l.getId());
//                        for (ObjectMetadata blob : blobs) {
//                            System.out.println("blob: " + blob);
//                        }
                        assertEquals(1, blobs.size());
                    }
                    assertTrue(ledgers.size() >= 1);
                }

                blobManager.gc();

                assertEquals(0, metadataManager.listDeletableLedgers(BUCKET_ID).size());

                blobManager.delete(BUCKET_ID, id).get();

                {
                    Collection<LedgerMetadata> ledgers = metadataManager.listLedgersbyBucketId(BUCKET_ID);
                    for (LedgerMetadata l : ledgers) {
//                        System.out.println("LedgerMetadata:" + l);
                        Collection<ObjectMetadata> blobs = metadataManager.listBlobsByLedger(BUCKET_ID, l.getId());
//                        for (ObjectMetadata blob : blobs) {
//                            System.out.println("blob: " + blob);
//                        }
                        assertEquals(0, blobs.size());
                    }
                    assertTrue(ledgers.size() >= 1);
                }

                assertEquals(1, metadataManager.listDeletableLedgers(BUCKET_ID).size());

                blobManager.gc();

                // the ledger is still open, it cannot be dropped
                assertEquals(1, metadataManager.listDeletableLedgers(BUCKET_ID).size());

                // force close all ledgers
                ((BookKeeperBlobManager) blobManager).closeAllActiveWriters();

                // now the ledger can be dropped
                blobManager.gc();

                assertEquals(0, metadataManager.listDeletableLedgers(BUCKET_ID).size());

            }
        }
    }
}
