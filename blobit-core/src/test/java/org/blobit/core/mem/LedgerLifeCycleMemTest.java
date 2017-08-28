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
package org.blobit.core.mem;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Collection;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import org.blobit.core.api.BucketConfiguration;
import org.blobit.core.api.Configuration;
import org.blobit.core.api.LedgerMetadata;
import org.blobit.core.api.ObjectManagerFactory;
import org.blobit.core.api.ObjectMetadata;
import org.junit.Assert;
import org.junit.Test;

import herddb.server.ServerConfiguration;

public class LedgerLifeCycleMemTest {

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

        Configuration configuration
            = new Configuration()
                .setType(Configuration.TYPE_MEM)
                .setConcurrentWriters(10);
        try (LocalManager manager = (LocalManager) ObjectManagerFactory.createObjectManager(configuration, null);) {
            long _start = System.currentTimeMillis();

            try {
                manager.put(BUCKET_ID, TEST_DATA).get();
                fail();
            } catch (ExecutionException ok) {
                ok.printStackTrace();
            }

            manager.createBucket(BUCKET_ID, BUCKET_ID, BucketConfiguration.DEFAULT);
            String id = manager.put(BUCKET_ID, TEST_DATA).get();
            Assert.assertArrayEquals(manager.get(BUCKET_ID, id).get(), TEST_DATA);

            {
                Collection<LedgerMetadata> ledgers = manager.listLedgersbyBucketId(BUCKET_ID);
                for (LedgerMetadata l : ledgers) {
                    System.out.println("LedgerMetadata:" + l);
                    Collection<ObjectMetadata> blobs = manager.listObjectsByLedger(BUCKET_ID, l.getId());
//                        for (ObjectMetadata blob : blobs) {
//                            System.out.println("blob: " + blob);
//                        }
                    assertEquals(1, blobs.size());
                }
                assertTrue(ledgers.size() >= 1);
            }

            manager.gc();

            assertEquals(0, manager.listDeletableLedgers(BUCKET_ID).size());

            manager.delete(BUCKET_ID, id).get();

            {
                Collection<LedgerMetadata> ledgers = manager.listLedgersbyBucketId(BUCKET_ID);
                for (LedgerMetadata l : ledgers) {
//                        System.out.println("LedgerMetadata:" + l);
                    Collection<ObjectMetadata> blobs = manager.listObjectsByLedger(BUCKET_ID, l.getId());
//                        for (ObjectMetadata blob : blobs) {
//                            System.out.println("blob: " + blob);
//                        }
                    assertEquals(0, blobs.size());
                }
                assertTrue(ledgers.size() >= 1);
            }

            assertEquals(1, manager.listDeletableLedgers(BUCKET_ID).size());

            manager.gc();

            assertEquals(0, manager.listDeletableLedgers(BUCKET_ID).size());

        }
    }

}
