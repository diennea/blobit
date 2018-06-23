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

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.blobit.core.api.BucketConfiguration;
import org.blobit.core.api.Configuration;
import org.blobit.core.api.ObjectManager;
import org.blobit.core.api.ObjectManagerFactory;
import org.blobit.core.api.PutPromise;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import herddb.jdbc.HerdDBEmbeddedDataSource;
import herddb.server.ServerConfiguration;
import io.netty.util.internal.PlatformDependent;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.blobit.core.api.BucketHandle;
import org.blobit.core.api.DownloadPromise;
import org.blobit.core.api.ObjectManagerException;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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
    private static final byte[] TEST_DATA = new byte[10 * 1024];

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
            try (ObjectManager manager = ObjectManagerFactory.createObjectManager(configuration, datasource);) {
                long _start = System.currentTimeMillis();

                manager.createBucket(BUCKET_ID, BUCKET_ID, BucketConfiguration.DEFAULT).get();
                BucketHandle bucket = manager.getBucket(BUCKET_ID);
                bucket.put(null, TEST_DATA).get();

                List<PutPromise> batch = new ArrayList<>();
                for (int i = 0; i < 1000; i++) {
                    batch.add(bucket.put(null, TEST_DATA));
                }
                List<String> ids = new ArrayList<>();
                for (PutPromise f : batch) {
                    ids.add(f.get());
                }
                for (String id : ids) {
                    bucket.delete(id).get();
                }

                long _stop = System.currentTimeMillis();
                double speed = (int) (batch.size() * 60_000.0 / (_stop - _start));
                double band = speed * TEST_DATA.length;
                long total = (batch.size() * TEST_DATA.length * 1L) / (1024 * 1024);
                System.out.println("TIME: " + (_stop - _start) + " ms for " + batch.size() + " blobs, total " + total + " MBs, " + speed + " blobs/h " + (band / 1e9) + " Gbytes/h");
            }
        }
    }

    @Test
    public void testNamedAPI() throws Exception {
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

                List<PutPromise> batch = new ArrayList<>();
                for (int i = 0; i < 1000; i++) {
                    batch.add(bucket.put("foo" + i, TEST_DATA));
                }
                List<String> ids = new ArrayList<>();
                for (PutPromise f : batch) {
                    ids.add(f.get());
                }
                for (int i = 0; i < 1000; i++) {
                    bucket.getByName("foo" + i).get();
                    bucket.downloadByName("foo" + i, (l) -> {
                    }, new ByteArrayOutputStream(), 0, -1).get();
                    bucket.deleteByName("foo" + i).get();
                }

                long _stop = System.currentTimeMillis();
                double speed = (int) (batch.size() * 60_000.0 / (_stop - _start));
                double band = speed * TEST_DATA.length;
                long total = (batch.size() * TEST_DATA.length * 1L) / (1024 * 1024);
                System.out.println("TIME: " + (_stop - _start) + " ms for " + batch.size() + " blobs, total " + total + " MBs, " + speed + " blobs/h " + (band / 1e9) + " Gbytes/h");
            }
        }
    }

    @Test
    public void testStreamingWritesStreamShortRead() throws Exception {
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

                try {
                    ByteArrayInputStream in = new ByteArrayInputStream(TEST_DATA);
                    PutPromise putResult = bucket.put(null, TEST_DATA.length * 2, in);
                    putResult.get();
                    fail();
                } catch (ObjectManagerException err) {
                    assertTrue(err.getCause() instanceof EOFException);
                }

                ByteArrayInputStream in = new ByteArrayInputStream(TEST_DATA);
                PutPromise putResult2 = bucket.put(null, TEST_DATA.length, in);
                putResult2.get();
            }
        }
    }

    @Test
    public void testStreamingWrites() throws Exception {
        Properties dsProperties = new Properties();
        dsProperties.put(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_LOCAL);
        try (ZKTestEnv env = new ZKTestEnv(tmp.newFolder("zk").toPath());
                HerdDBEmbeddedDataSource datasource = new HerdDBEmbeddedDataSource(dsProperties)) {
            env.startBookie();
            Configuration configuration
                    = new Configuration()
                            .setType(Configuration.TYPE_BOOKKEEPER)
                            .setConcurrentWriters(4)
                            .setMaxEntrySize(TEST_DATA.length / 2 - 1)
                            .setZookeeperUrl(env.getAddress());
            try (ObjectManager manager = ObjectManagerFactory.createObjectManager(configuration, datasource);) {
                long _start = System.currentTimeMillis();

                manager.createBucket(BUCKET_ID, BUCKET_ID, BucketConfiguration.DEFAULT).get();
                BucketHandle bucket = manager.getBucket(BUCKET_ID);

                int[] testcases = {0, TEST_DATA.length * 2 /* too big */, 10, 1040, TEST_DATA.length};
                List<PutPromise> results = new ArrayList<>();
                for (int size : testcases) {
                    ByteArrayInputStream in = new ByteArrayInputStream(TEST_DATA);
                    PutPromise putResult = bucket.put(null, size, in);
                    results.add(putResult);
                }
                for (int i = 0; i < testcases.length; i++) {
                    PutPromise result = results.get(i);
                    if (testcases[i] > TEST_DATA.length) {
                        try {
                            result.get();
                            fail();
                        } catch (ObjectManagerException err) {
                            // OKAY
                        }
                    } else {
                        result.get();
                    }
                }

                for (int i = 0; i < testcases.length; i++) {
                    int expectedSize = testcases[i];
                    String id = results.get(i).id;
                    if (id != null) { // failed writes do not carry id an id                        
                        byte[] data = bucket.get(id).get();
                        assertEquals(expectedSize, data.length);
//                        Arrays.equals(TEST_DATA, 0, expectedSize, data, 0, data.length);
                        PlatformDependent.equals(TEST_DATA, 0, data, 0, expectedSize);
                    } else {
                        assertTrue(expectedSize > TEST_DATA.length);
                    }

                }

                for (PutPromise result : results) {
                    if (result.id != null) {
                        bucket.delete(result.id);
                    }
                }
            }
        }
    }

    @Test
    public void testStreamingReads() throws Exception {

        Properties dsProperties = new Properties();
        dsProperties.put(ServerConfiguration.PROPERTY_MODE, ServerConfiguration.PROPERTY_MODE_LOCAL);
        try (ZKTestEnv env = new ZKTestEnv(tmp.newFolder("zk").toPath());
                HerdDBEmbeddedDataSource datasource = new HerdDBEmbeddedDataSource(dsProperties)) {
            env.startBookie();
            Configuration configuration
                    = new Configuration()
                            .setType(Configuration.TYPE_BOOKKEEPER)
                            .setConcurrentWriters(4)
                            .setMaxEntrySize(64 * 1024)
                            .setZookeeperUrl(env.getAddress());
            try (ObjectManager manager = ObjectManagerFactory.createObjectManager(configuration, datasource);) {
                long _start = System.currentTimeMillis();

                manager.createBucket(BUCKET_ID, BUCKET_ID, BucketConfiguration.DEFAULT).get();
                BucketHandle bucket = manager.getBucket(BUCKET_ID);

                int[] testdatasizes = {
                    0,
                    10,
                    configuration.getMaxEntrySize(),
                    configuration.getMaxEntrySize() + 1,
                    configuration.getMaxEntrySize() * 2,
                    configuration.getMaxEntrySize() * 2 + 1,
                    configuration.getMaxEntrySize() * 3 - 1,
                    configuration.getMaxEntrySize() * 3,
                    configuration.getMaxEntrySize() * 3 + 2
                };

                for (int testdatasize : testdatasizes) {
                    final byte[] testdata = new byte[testdatasize];
                    Random random = new Random();
                    random.nextBytes(testdata);

                    String id = bucket.put(null, testdata).get();

                    int[] offsets = {0,
                        10,
                        configuration.getMaxEntrySize() / 2,
                        configuration.getMaxEntrySize(),
                        configuration.getMaxEntrySize() * 2 - 1,
                        configuration.getMaxEntrySize() * 2,
                        configuration.getMaxEntrySize() * 2 + 1,
                        testdata.length / 2,
                        testdata.length,
                        testdata.length + 100};

                    for (int offset : offsets) {
                        int[] maxLengths = {
                            0, /* no read ? */
                            10,
                            1040,
                            configuration.getMaxEntrySize() + 10 /* second entry */,
                            configuration.getMaxEntrySize() * 2, /* bad value (not working yet) */
                            testdata.length,
                            testdata.length + 100 /* bigger than original len*/};
                        List<DownloadPromise> results = new ArrayList<>();
                        List<ByteArrayOutputStream> resultStreams = new ArrayList<>();
                        List<AtomicLong> contentLengths = new ArrayList<>();
                        for (int maxLength : maxLengths) {
                            ByteArrayOutputStream out = new ByteArrayOutputStream();
                            AtomicLong dataToReceive = new AtomicLong();
                            DownloadPromise putResult = bucket.download(id, dataToReceive::set, out, offset, maxLength);
                            results.add(putResult);
                            resultStreams.add(out);
                            contentLengths.add(dataToReceive);
                        }
                        for (int i = 0; i < maxLengths.length; i++) {
                            int originalExpectedSize = maxLengths[i];

                            DownloadPromise downloadPromise = results.get(i);
                            ByteArrayOutputStream stream = resultStreams.get(i);

                            AtomicLong contentLength = contentLengths.get(i);
                            // wait for download to complete
                            downloadPromise.get();
                            int expectedSize = originalExpectedSize;
                            if (offset > testdata.length) {
                                expectedSize = 0;
                            }
                            if (expectedSize > 0 && expectedSize > testdata.length - offset) {
                                expectedSize = testdata.length - offset;
                            }
                            byte[] data = stream.toByteArray();
                            LOG.info("testcase datasize " + testdata.length + ", offset " + offset + ", " + downloadPromise.id + " originalExpectedSize " + originalExpectedSize + ", expected size " + expectedSize + " ->  (object len " + testdata.length + ") actual " + data.length);

                            assertEquals(expectedSize, data.length);
                            if (offset >= testdata.length) {
                                assertEquals(0, data.length);
                            } else {
                                PlatformDependent.equals(testdata, offset, data, 0, expectedSize);
                            }
                            assertEquals(expectedSize, contentLength.intValue());
                            
                        }
                    }
                }

            }
        }
    }
    private static final Logger LOG = Logger.getLogger(SimpleClusterWriterTest.class.getName());

    @Test
    public void testEmptyBlob() throws Exception {
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

                manager.createBucket(BUCKET_ID, BUCKET_ID, BucketConfiguration.DEFAULT).get();
                BucketHandle bucket = manager.getBucket(BUCKET_ID);
                PutPromise put = bucket.put(null, new byte[0]);

                FutureUtils.result(put.future);

                byte[] read = bucket.get(put.id).get();
                assertEquals(0, read.length);

                bucket.delete(put.id).get();
            }
        }
    }
}
