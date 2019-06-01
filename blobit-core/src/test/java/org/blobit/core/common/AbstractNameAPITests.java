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
package org.blobit.core.common;

import herddb.jdbc.HerdDBEmbeddedDataSource;
import herddb.server.ServerConfiguration;
import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import org.blobit.core.api.BucketConfiguration;
import org.blobit.core.api.BucketHandle;
import org.blobit.core.api.Configuration;
import org.blobit.core.api.NamedObjectMetadata;
import org.blobit.core.api.ObjectManager;
import org.blobit.core.api.ObjectManagerFactory;
import org.blobit.core.cluster.ZKTestEnv;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Common tests about the Named API
 *
 * @author eolivelli
 */
public class AbstractNameAPITests {

    @Rule
    public final TemporaryFolder tmp = new TemporaryFolder();

    private static final String BUCKET_ID = "mybucket";
    private static final byte[] TEST_DATA = new byte[10 * 1024];
    private static final byte[] TEST_DATA2 = new byte[10 * 1024];

    static {
        Random random = new Random();
        random.nextBytes(TEST_DATA);
        random.nextBytes(TEST_DATA2);
    }

    @Test
    public void testAppend() throws Exception {
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
            try (ObjectManager manager = ObjectManagerFactory.createObjectManager(configuration, datasource);) {
                long _start = System.currentTimeMillis();

                manager.createBucket(BUCKET_ID, BUCKET_ID, BucketConfiguration.DEFAULT).get();
                BucketHandle bucket = manager.getBucket(BUCKET_ID);
                String name = "foo";
                String firstObjectId = bucket.put(name, TEST_DATA).get();

                // create an unnamed blob
                String secondObjectId = bucket.put(null, TEST_DATA).get();
                bucket.append(secondObjectId, name);

                NamedObjectMetadata objectMd = bucket.statByName(name);
                assertEquals(2, objectMd.getNumObjects());
                assertEquals(name, objectMd.getName());
                assertEquals(firstObjectId, objectMd.getObject(0).id);
                assertEquals(secondObjectId, objectMd.getObject(0).id);
                assertEquals(TEST_DATA.length + TEST_DATA2.length, objectMd.getSize());

                List<byte[]> result = bucket.getByName(name).get();
                assertArrayEquals(TEST_DATA, result.get(0));
                assertArrayEquals(TEST_DATA2, result.get(1));

                {
                    // download with length = -1
                    ByteArrayOutputStream oo = new ByteArrayOutputStream();
                    AtomicLong size = new AtomicLong();
                    byte[] resultArray = oo.toByteArray();
                    bucket.downloadByName(name, size::set, oo, 0, -1).get();
                    assertTrue(Arrays.equals(TEST_DATA, 0, TEST_DATA.length,
                            resultArray, 0, TEST_DATA.length));
                    assertEquals(TEST_DATA.length + TEST_DATA2.length, size.get());
                    assertTrue(Arrays.equals(TEST_DATA2, 0, TEST_DATA2.length,
                            resultArray, TEST_DATA.length, TEST_DATA.length + TEST_DATA2.length));
                }
                {
                    // download with given length
                    ByteArrayOutputStream oo = new ByteArrayOutputStream();
                    AtomicLong size = new AtomicLong();
                    byte[] resultArray = oo.toByteArray();
                    bucket.downloadByName(name, size::set, oo, 0, objectMd.getSize()).get();
                    assertEquals(TEST_DATA.length + TEST_DATA2.length, size.get());
                    assertTrue(Arrays.equals(TEST_DATA, 0, TEST_DATA.length,
                            resultArray, 0, TEST_DATA.length));
                    assertTrue(Arrays.equals(TEST_DATA2, 0, TEST_DATA2.length,
                            resultArray, TEST_DATA.length, TEST_DATA.length + TEST_DATA2.length));
                }

                {
                    // download with offset starting at second part, len = -1
                    ByteArrayOutputStream oo = new ByteArrayOutputStream();
                    AtomicLong size = new AtomicLong();
                    byte[] resultArray = oo.toByteArray();
                    bucket.downloadByName(name, size::set, oo, TEST_DATA.length, -1).get();
                    assertEquals(TEST_DATA2.length, size.get());
                    assertTrue(Arrays.equals(TEST_DATA2, 0, TEST_DATA2.length,
                            resultArray, 0, TEST_DATA2.length));
                }

                {
                    // download with offset starting at second part, len = -1
                    ByteArrayOutputStream oo = new ByteArrayOutputStream();
                    AtomicLong size = new AtomicLong();
                    byte[] resultArray = oo.toByteArray();
                    bucket.downloadByName(name, size::set, oo, TEST_DATA.length, TEST_DATA2.length).get();
                    assertEquals(TEST_DATA2.length, size.get());
                    assertTrue(Arrays.equals(TEST_DATA2, 0, TEST_DATA2.length,
                            resultArray, 0, TEST_DATA2.length));
                }
            }
        }
    }

}
