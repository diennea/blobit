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

import java.util.Properties;
import java.util.Random;

import org.blobit.core.api.BucketConfiguration;
import org.blobit.core.api.Configuration;
import org.blobit.core.api.ObjectManagerFactory;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import herddb.jdbc.HerdDBEmbeddedDataSource;
import herddb.server.ServerConfiguration;
import java.sql.Connection;
import org.blobit.core.api.BucketHandle;
import org.blobit.core.api.ObjectManager;
import static org.junit.Assert.assertNull;
import org.junit.runners.model.Statement;

public class DeleteBucketTest {

    @Rule
    public final TemporaryFolder tmp = new TemporaryFolder();

    private static final String BUCKET_ID = "mybucket";
    private static final byte[] TEST_DATA = new byte[100 * 1024];

    static {
        Random random = new Random();
        random.nextBytes(TEST_DATA);
    }

    @Test
    public void testCleanupUsingTablespaces() throws Exception {
        Properties dsProperties = new Properties();
        dsProperties.put(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_LOCAL);
        try (ZKTestEnv env = new ZKTestEnv(tmp.newFolder("zk").toPath());
            HerdDBEmbeddedDataSource datasource = new HerdDBEmbeddedDataSource(dsProperties)) {
            env.startBookie();
            Configuration configuration
                = new Configuration()
                    .setType(Configuration.TYPE_BOOKKEEPER)
                    .setConcurrentWriters(10)
                    .setUseTablespaces(true)
                    .setZookeeperUrl(env.getAddress());
            try (ObjectManager manager = (ClusterObjectManager) ObjectManagerFactory.createObjectManager(configuration, datasource);) {
                manager.createBucket(BUCKET_ID, BUCKET_ID, BucketConfiguration.DEFAULT).get();
                BucketHandle bucket = manager.getBucket(BUCKET_ID);
                String id = bucket.put(null, TEST_DATA).get();
                Assert.assertArrayEquals(bucket.get(id).get(), TEST_DATA);
                manager.deleteBucket(BUCKET_ID).get();
                assertNull(manager.getBucketMetadata(BUCKET_ID));

                manager.createBucket(BUCKET_ID, BUCKET_ID, BucketConfiguration.DEFAULT).get();

                // drop the tablespace, simulating an interrupted delete
                try (Connection connection = datasource.getConnection();
                    java.sql.Statement s = connection.createStatement()) {
                    s.executeUpdate("DROP TABLESPACE '" + BUCKET_ID + "'");
                }
                manager.cleanup();
            }
        }
    }

    @Test
    public void testCleanupWithoutTablespaces() throws Exception {
        Properties dsProperties = new Properties();
        dsProperties.put(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_LOCAL);
        try (ZKTestEnv env = new ZKTestEnv(tmp.newFolder("zk").toPath());
            HerdDBEmbeddedDataSource datasource = new HerdDBEmbeddedDataSource(dsProperties)) {
            env.startBookie();
            Configuration configuration
                = new Configuration()
                    .setType(Configuration.TYPE_BOOKKEEPER)
                    .setConcurrentWriters(10)
                    .setUseTablespaces(false)
                    .setZookeeperUrl(env.getAddress());
            try (ObjectManager manager = (ClusterObjectManager) ObjectManagerFactory.createObjectManager(configuration, datasource);) {
                manager.createBucket(BUCKET_ID, BUCKET_ID, BucketConfiguration.DEFAULT).get();
                BucketHandle bucket = manager.getBucket(BUCKET_ID);
                String id = bucket.put(null, TEST_DATA).get();
                Assert.assertArrayEquals(bucket.get(id).get(), TEST_DATA);
                manager.deleteBucket(BUCKET_ID).get();
                assertNull(manager.getBucketMetadata(BUCKET_ID));
                manager.cleanup();
                manager.cleanup();
                manager.cleanup();
            }
        }
    }
}
