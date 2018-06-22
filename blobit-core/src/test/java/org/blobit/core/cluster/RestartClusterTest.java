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
import java.nio.file.Path;
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
import org.blobit.core.api.BucketHandle;

public class RestartClusterTest {

    @Rule
    public final TemporaryFolder tmp = new TemporaryFolder(new File("target").getAbsoluteFile());

    private static final String BUCKET_ID = "mybucket";
    private static final byte[] TEST_DATA = new byte[100 * 1024];

    static {
        Random random = new Random();
        random.nextBytes(TEST_DATA);
    }

    @Test
    public void testRestart() throws Exception {
        Properties dsProperties = new Properties();
        dsProperties.put(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_LOCAL);

        Path path = tmp.newFolder("zk").toPath();

        String insertedID;

        try (ZKTestEnv env = new ZKTestEnv(path);
                HerdDBEmbeddedDataSource datasource = new HerdDBEmbeddedDataSource(dsProperties)) {
            env.startBookie();
            Configuration configuration = new Configuration().setType(Configuration.TYPE_BOOKKEEPER)
                    .setConcurrentWriters(10).setZookeeperUrl(env.getAddress());

            /* First ObjectManager */
            try (ObjectManager manager = ObjectManagerFactory.createObjectManager(configuration, datasource);) {
                manager.createBucket(BUCKET_ID, BUCKET_ID, BucketConfiguration.DEFAULT).get();
                BucketHandle bucket = manager.getBucket(BUCKET_ID);
                PutPromise put = bucket.put(null, TEST_DATA);
                insertedID = put.get();
                Assert.assertArrayEquals(TEST_DATA, bucket.get(insertedID).get());
            }

            /* Second ObjectManager */
            try (ObjectManager manager = ObjectManagerFactory.createObjectManager(configuration, datasource);) {
                BucketHandle bucket = manager.getBucket(BUCKET_ID);
                Assert.assertArrayEquals(TEST_DATA, bucket.get(insertedID).get());
            }
        }
    }
}
