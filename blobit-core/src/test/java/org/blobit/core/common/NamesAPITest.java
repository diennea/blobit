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

import static org.blobit.core.filters.NamedObjectFilters.nameStartsWith;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import herddb.jdbc.HerdDBEmbeddedDataSource;
import herddb.server.ServerConfiguration;
import herddb.utils.CompareBytesUtils;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import org.blobit.core.api.BucketConfiguration;
import org.blobit.core.api.BucketHandle;
import org.blobit.core.api.Configuration;
import org.blobit.core.api.NamedObjectDeletePromise;
import org.blobit.core.api.NamedObjectMetadata;
import org.blobit.core.api.ObjectAlreadyExistsException;
import org.blobit.core.api.ObjectManager;
import org.blobit.core.api.ObjectManagerException;
import org.blobit.core.api.ObjectManagerFactory;
import org.blobit.core.api.ObjectMetadata;
import org.blobit.core.api.ObjectNotFoundException;
import org.blobit.core.api.PutOptions;
import org.blobit.core.cluster.ZKTestEnv;
import org.blobit.core.util.TestUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Common tests about the Named API
 *
 * @author eolivelli
 */
public class NamesAPITest {

    @Rule
    public final TemporaryFolder tmp = new TemporaryFolder();

    private static final String BUCKET_ID = "mybucket";
    private static final byte[] TEST_DATA = new byte[7];
    private static final byte[] TEST_DATA2 = new byte[10];
    private static final byte[] TEST_DATA3 = new byte[8];
    private static final byte[] TEST_DATA4 = new byte[4];

    static {
        Random random = new Random();
        random.nextBytes(TEST_DATA);
        random.nextBytes(TEST_DATA2);
        random.nextBytes(TEST_DATA3);
        random.nextBytes(TEST_DATA4);
    }

    @Test
    public void testClusterImpl() throws Exception {
        Properties dsProperties = new Properties();
        dsProperties.put(ServerConfiguration.PROPERTY_MODE,
                ServerConfiguration.PROPERTY_MODE_LOCAL);
        try (ZKTestEnv env = new ZKTestEnv(tmp.newFolder("zk").toPath());
                HerdDBEmbeddedDataSource datasource =
                new HerdDBEmbeddedDataSource(
                        dsProperties)) {
            env.startBookie();
            Configuration configuration =
                    new Configuration()
                            .setType(Configuration.TYPE_BOOKKEEPER)
                            .setConcurrentWriters(4)
                            .setZookeeperUrl(env.getAddress());
            try (ObjectManager manager = ObjectManagerFactory.
                    createObjectManager(configuration, datasource);) {
                testNamedApi(manager);
            }
        }
    }

    @Test
    public void testMemoryImpl() throws Exception {

        Configuration configuration =
                new Configuration()
                        .setType(Configuration.TYPE_MEM)
                        .setConcurrentWriters(4);
        try (ObjectManager manager = ObjectManagerFactory.createObjectManager(
                configuration, null);) {
            testNamedApi(manager);
        }

    }

    private void testNamedApi(final ObjectManager manager) throws ObjectManagerException, InterruptedException,
            ExecutionException {
        manager.createBucket(BUCKET_ID, BUCKET_ID, BucketConfiguration.DEFAULT).
                get();
        BucketHandle bucket = manager.getBucket(BUCKET_ID);
        String name = "foo";
        String firstObjectId = bucket.put(name, TEST_DATA).get();

        String secondObjectId = bucket.put(name,
                TEST_DATA2, 0, TEST_DATA2.length, PutOptions.APPEND).get();

        NamedObjectMetadata objectMd = bucket.statByName(name);
        assertEquals(2, objectMd.getNumObjects());
        assertEquals(name, objectMd.getName());
        assertEquals(firstObjectId, objectMd.getObject(0).id);
        assertEquals(secondObjectId, objectMd.getObject(1).id);
        assertEquals(TEST_DATA.length + TEST_DATA2.length, objectMd.getSize());

        List<byte[]> result = bucket.getByName(name).get();
        assertArrayEquals(TEST_DATA, result.get(0));
        assertArrayEquals(TEST_DATA2, result.get(1));

        {
            // download with length = -1
            ByteArrayOutputStream oo = new ByteArrayOutputStream();
            AtomicLong size = new AtomicLong();
            bucket.downloadByName(name, size::set, oo, 0, -1).get();
            byte[] resultArray = oo.toByteArray();
            assertEquals(TEST_DATA.length + TEST_DATA2.length, size.get());
            assertEquals(TEST_DATA.length + TEST_DATA2.length,
                    resultArray.length);
            assertTrue(CompareBytesUtils.arraysEquals(TEST_DATA, 0, TEST_DATA.length,
                    resultArray, 0, TEST_DATA.length));
            assertTrue(CompareBytesUtils.arraysEquals(TEST_DATA2, 0, TEST_DATA2.length,
                    resultArray, TEST_DATA.length,
                    TEST_DATA.length + TEST_DATA2.length));
        }
        {
            // download with given length
            ByteArrayOutputStream oo = new ByteArrayOutputStream();
            AtomicLong size = new AtomicLong();
            bucket.downloadByName(name, size::set, oo, 0, objectMd.getSize()).
                    get();
            byte[] resultArray = oo.toByteArray();
            assertEquals(TEST_DATA.length + TEST_DATA2.length, size.get());
            assertEquals(TEST_DATA.length + TEST_DATA2.length,
                    resultArray.length);
            assertTrue(CompareBytesUtils.arraysEquals(TEST_DATA, 0, TEST_DATA.length,
                    resultArray, 0, TEST_DATA.length));
            assertTrue(CompareBytesUtils.arraysEquals(TEST_DATA2, 0, TEST_DATA2.length,
                    resultArray, TEST_DATA.length,
                    TEST_DATA.length + TEST_DATA2.length));
        }

        {
            // download with offset starting at second part, len = -1
            ByteArrayOutputStream oo = new ByteArrayOutputStream();
            AtomicLong size = new AtomicLong();
            bucket.downloadByName(name, size::set, oo, TEST_DATA.length, -1).
                    get();
            byte[] resultArray = oo.toByteArray();
            assertEquals(TEST_DATA2.length, size.get());
            assertEquals(TEST_DATA2.length, resultArray.length);
            assertTrue(CompareBytesUtils.arraysEquals(TEST_DATA2, 0, TEST_DATA2.length,
                    resultArray, 0, TEST_DATA2.length));
        }

        {
            // download with offset starting at second part, len = fixed
            ByteArrayOutputStream oo = new ByteArrayOutputStream();
            AtomicLong size = new AtomicLong();
            bucket.downloadByName(name, size::set, oo, TEST_DATA.length,
                    TEST_DATA2.length).get();
            byte[] resultArray = oo.toByteArray();
            assertEquals(TEST_DATA2.length, size.get());
            assertEquals(TEST_DATA2.length, resultArray.length);
            assertTrue(CompareBytesUtils.arraysEquals(TEST_DATA2, 0, TEST_DATA2.length,
                    resultArray, 0, TEST_DATA2.length));
        }

        {
            // download with offset = total len, len = -1
            ByteArrayOutputStream oo = new ByteArrayOutputStream();
            AtomicLong size = new AtomicLong();
            bucket.downloadByName(name, size::set, oo,
                    TEST_DATA.length + TEST_DATA2.length, -1).get();
            byte[] resultArray = oo.toByteArray();
            assertEquals(0, size.get());
            assertEquals(0, resultArray.length);
        }
        {
            // download with offset = total len, len = 0
            ByteArrayOutputStream oo = new ByteArrayOutputStream();
            AtomicLong size = new AtomicLong();
            bucket.downloadByName(name, size::set, oo,
                    TEST_DATA.length + TEST_DATA2.length, 0).get();
            byte[] resultArray = oo.toByteArray();
            assertEquals(0, size.get());
            assertEquals(0, resultArray.length);
        }
        {
            // download from two segments, fixed len
            ByteArrayOutputStream oo = new ByteArrayOutputStream();
            AtomicLong size = new AtomicLong();
            bucket.downloadByName(name, size::set, oo, TEST_DATA.length - 2,
                    5).get();
            byte[] resultArray = oo.toByteArray();
            assertEquals(5, size.get());
            assertEquals(5, resultArray.length);
            assertTrue(CompareBytesUtils.arraysEquals(TEST_DATA, TEST_DATA.length - 2,
                    TEST_DATA.length,
                    resultArray, 0, 2));
            assertTrue(CompareBytesUtils.arraysEquals(TEST_DATA2, 0, 3,
                    resultArray, 2, 5));
        }

        {
            // ask for too much data, serve the full blob
            ByteArrayOutputStream oo = new ByteArrayOutputStream();
            AtomicLong size = new AtomicLong();
            bucket.downloadByName(name, size::set, oo, 0,
                    TEST_DATA.length + TEST_DATA2.length + 10).get();
            byte[] resultArray = oo.toByteArray();
            assertEquals(TEST_DATA.length + TEST_DATA2.length, size.get());
            assertEquals(TEST_DATA.length + TEST_DATA2.length,
                    resultArray.length);
            assertTrue(CompareBytesUtils.arraysEquals(TEST_DATA, 0, TEST_DATA.length,
                    resultArray, 0, TEST_DATA.length));
            assertTrue(CompareBytesUtils.arraysEquals(TEST_DATA2, 0, TEST_DATA2.length,
                    resultArray, TEST_DATA.length,
                    TEST_DATA.length + TEST_DATA2.length));
        }

        {
            // ask for too much data with an offset, serve the full blob
            ByteArrayOutputStream oo = new ByteArrayOutputStream();
            AtomicLong size = new AtomicLong();
            bucket.downloadByName(name, size::set, oo, 3,
                    TEST_DATA.length + TEST_DATA2.length + 10).get();
            byte[] resultArray = oo.toByteArray();
            assertEquals(TEST_DATA.length + TEST_DATA2.length - 3, size.get());
            assertEquals(TEST_DATA.length + TEST_DATA2.length - 3,
                    resultArray.length);
            assertTrue(CompareBytesUtils.arraysEquals(TEST_DATA, 3, TEST_DATA.length,
                    resultArray, 0, TEST_DATA.length - 3));
            assertTrue(CompareBytesUtils.arraysEquals(TEST_DATA2, 0, TEST_DATA2.length,
                    resultArray, TEST_DATA.length - 3,
                    TEST_DATA.length + TEST_DATA2.length - 3));
        }

        NamedObjectDeletePromise deleteHandle = bucket.deleteByName(name);
        assertEquals(name, deleteHandle.name);
        assertEquals(firstObjectId, deleteHandle.id.get(0));
        assertEquals(secondObjectId, deleteHandle.id.get(1));
        deleteHandle.get();

        // try download
        {
            try {
                ByteArrayOutputStream oo = new ByteArrayOutputStream();
                AtomicLong size = new AtomicLong();
                bucket.downloadByName(name, size::set, oo, 0, -1).get();
                fail("cannot download a deleted object");
            } catch (ObjectManagerException err) {
                assertThat(err, instanceOf(ObjectNotFoundException.class));
            }
        }
        // try get
        {
            try {
                bucket.getByName(name).get();
                fail("cannot get a deleted object");
            } catch (ObjectManagerException err) {
                assertThat(err, instanceOf(ObjectNotFoundException.class));
            }
        }

        // append again (appending to a not existing object will make the object exist)
        String thirdObjectId = bucket.put(name, TEST_DATA3, 0, TEST_DATA3.length, PutOptions.APPEND).get();
        String thirdObjectIdb = bucket.put(name, TEST_DATA3, 0, TEST_DATA3.length, PutOptions.APPEND).get();
        String fourthObjectId = bucket.put(name, TEST_DATA4, 0, TEST_DATA4.length, PutOptions.APPEND).get();
        String thirdObjectIdc = bucket.put(name, TEST_DATA3, 0, TEST_DATA3.length, PutOptions.APPEND).get();

        List<byte[]> values = bucket.getByName(name).get();
        assertEquals(4, values.size());

        assertArrayEquals(TEST_DATA3, values.get(0));
        assertArrayEquals(TEST_DATA3, values.get(1));
        assertArrayEquals(TEST_DATA4, values.get(2));
        assertArrayEquals(TEST_DATA3, values.get(3));
        NamedObjectMetadata stat = bucket.statByName(name);
        assertEquals(thirdObjectId, stat.getObject(0).getId());
        assertEquals(thirdObjectIdb, stat.getObject(1).getId());
        assertEquals(fourthObjectId, stat.getObject(2).getId());
        assertEquals(thirdObjectIdc, stat.getObject(3).getId());

        // bad guy ! delete the raw object, without deleting the NamedObject
        bucket.delete(fourthObjectId);

        try {
            // the get will fail, downoadByName may work partially
            bucket.getByName(name).get();
        } catch (ObjectManagerException err) {
            assertThat(err, instanceOf(ObjectNotFoundException.class));
        }

        // final clean up
        bucket.deleteByName(name).get();
        assertNull(bucket.statByName(name));

        // empty blobs
        String empty1 = bucket.put("empty-named-object", new byte[0], 0, 0, PutOptions.APPEND).get();
        ObjectMetadata statEmpty1 = bucket.stat(empty1);
        assertEquals(0, statEmpty1.getSize());

        String empty2 = bucket.put("empty-named-object", new byte[0], 0, 0, PutOptions.APPEND).get();
        ObjectMetadata statEmpty2 = bucket.stat(empty2);
        assertEquals(0, statEmpty2.getSize());

        NamedObjectMetadata statEmptyNamedObject = bucket.statByName("empty-named-object");
        assertEquals(0, statEmptyNamedObject.getSize());
        assertEquals(2, statEmptyNamedObject.getNumObjects());

        // concat tests
        bucket.put("source-object", TEST_DATA).get();
        bucket.put("dest-object", TEST_DATA2).get();
        NamedObjectMetadata source1 = bucket.statByName("source-object");
        assertEquals(1, source1.getNumObjects());
        assertEquals(TEST_DATA.length, source1.getSize());
        assertEquals(TEST_DATA.length, source1.getObject(0).size);
        NamedObjectMetadata dest1 = bucket.statByName("dest-object");
        assertEquals(1, dest1.getNumObjects());
        assertEquals(TEST_DATA2.length, dest1.getSize());
        assertEquals(TEST_DATA2.length, dest1.getObject(0).size);

        bucket.concat("source-object", "dest-object");
        // source must not exist anymore
        NamedObjectMetadata existSource = bucket.statByName("source-object");
        assertNull(existSource);
        // assert dest status
        NamedObjectMetadata dest2 = bucket.statByName("dest-object");
        assertEquals(2, dest2.getNumObjects());
        assertEquals(dest2.getObject(0), dest1.getObject(0));
        assertEquals(dest2.getObject(1), source1.getObject(0));
        assertEquals(source1.getSize() + dest1.getSize(), dest2.getSize());

        // overwrite tests
        String obj1 = bucket.put("object-to-overwrite", TEST_DATA, 0, TEST_DATA.length,
                PutOptions.DEFAULT_OPTIONS).get();
        String obj2 = bucket.put("object-to-overwrite", TEST_DATA, 0, TEST_DATA.length,
                PutOptions.APPEND).get();
        NamedObjectMetadata toOverwrite1 = bucket.statByName("object-to-overwrite");
        assertEquals(2, toOverwrite1.getNumObjects());
        assertEquals(obj1, toOverwrite1.getObject(0).getId());
        assertEquals(obj2, toOverwrite1.getObject(1).getId());

        String obj2b = bucket.put("object-to-overwrite", TEST_DATA, 0, TEST_DATA.length,
                PutOptions.APPEND).get();
        NamedObjectMetadata toOverwrite1b = bucket.statByName("object-to-overwrite");
        assertEquals(3, toOverwrite1b.getNumObjects());
        assertEquals(obj1, toOverwrite1b.getObject(0).getId());
        assertEquals(obj2, toOverwrite1b.getObject(1).getId());
        assertEquals(obj2b, toOverwrite1b.getObject(2).getId());

        String obj3 = bucket.put("object-to-overwrite", TEST_DATA, 0, TEST_DATA.length, PutOptions.OVERWRITE).get();
        NamedObjectMetadata toOverwrite2 = bucket.statByName("object-to-overwrite");
        assertEquals(1, toOverwrite2.getNumObjects());
        assertEquals(obj3, toOverwrite2.getObject(0).getId());

        // append in streaming mode
        String obj4 = bucket.put("object-to-overwrite", TEST_DATA.length,
                new ByteArrayInputStream(TEST_DATA), PutOptions.OVERWRITE).get();
        NamedObjectMetadata toOverwrite3 = bucket.statByName("object-to-overwrite");
        assertEquals(1, toOverwrite3.getNumObjects());
        assertEquals(obj4, toOverwrite3.getObject(0).getId());

        TestUtils.assertThrows(ObjectAlreadyExistsException.class, () -> {
            bucket.put("object-to-overwrite", TEST_DATA).get();
        });

        bucket.put("/mydirectory/file1", TEST_DATA).get();
        bucket.put("/mydirectory/file2", TEST_DATA).get();
        bucket.put("/mydirectory/file2", TEST_DATA2, 0, TEST_DATA2.length, PutOptions.APPEND).get();
        bucket.put("/mydirectory/file3", TEST_DATA).get();

        List<NamedObjectMetadata> list = new ArrayList<>();
        bucket.listByName(nameStartsWith("/mydirectory"), o -> {
            list.add(o);
            return true;
        });
        assertEquals(3, list.size());
        for (NamedObjectMetadata md : list) {
            switch (md.getName()) {
                case "/mydirectory/file1":
                case "/mydirectory/file3":
                    assertEquals(TEST_DATA.length, md.getSize());
                    assertEquals(1, md.getNumObjects());
                    break;
                case "/mydirectory/file2":
                    assertEquals(TEST_DATA.length + TEST_DATA2.length, md.getSize());
                    assertEquals(2, md.getNumObjects());
                    break;
                default:
                    fail();
            }
        }

        bucket.put("/mydirectory/file3", TEST_DATA2, 0, TEST_DATA2.length, PutOptions.APPEND).get();

        List<NamedObjectMetadata> list2 = new ArrayList<>();
        bucket.listByName(nameStartsWith("/mydirectory"), o -> {
            list2.add(o);
            return true;
        });
        assertEquals(3, list2.size());
        for (NamedObjectMetadata md : list2) {
            switch (md.getName()) {
                case "/mydirectory/file1":
                    assertEquals(TEST_DATA.length, md.getSize());
                    assertEquals(1, md.getNumObjects());
                    break;
                case "/mydirectory/file2":
                case "/mydirectory/file3":
                    assertEquals(TEST_DATA.length + TEST_DATA2.length, md.getSize());
                    assertEquals(2, md.getNumObjects());
                    break;
                default:
                    fail();
            }
        }

        List<NamedObjectMetadata> list3 = new ArrayList<>();
        bucket.listByName(nameStartsWith("/empty-results"), o -> {
            list3.add(o);
            return true;
        });
        assertTrue(list3.isEmpty());

        // single file, with one blob
        List<NamedObjectMetadata> list4 = new ArrayList<>();
        bucket.listByName(nameStartsWith("/mydirectory/file1"), o -> {
            list4.add(o);
            return true;
        });
        assertEquals(1, list4.size());
        assertEquals(1, list4.get(0).getNumObjects());

        // single file, with two blob
        List<NamedObjectMetadata> list5 = new ArrayList<>();
        bucket.listByName(nameStartsWith("/mydirectory/file3"), o -> {
            list5.add(o);
            return true;
        });
        assertEquals(1, list5.size());
        assertEquals(2, list5.get(0).getNumObjects());

        bucket.put("/mydirectory/empty-file", new byte[0]).get();

        // single file, with one empty blob
        List<NamedObjectMetadata> list6 = new ArrayList<>();
        bucket.listByName(nameStartsWith("/mydirectory/empty-file"), o -> {
            list6.add(o);
            return true;
        });
        assertEquals(1, list6.size());
        assertEquals(1, list6.get(0).getNumObjects());
        assertEquals(0, list6.get(0).getSize());

        // append an empty blobs
        bucket.put("/mydirectory/empty-file", new byte[0], 0, 0, PutOptions.APPEND).get();

        // single file, with two empty blobs
        List<NamedObjectMetadata> list7 = new ArrayList<>();
        bucket.listByName(nameStartsWith("/mydirectory/empty-file"), o -> {
            list7.add(o);
            return true;
        });
        assertEquals(1, list7.size());
        assertEquals(2, list7.get(0).getNumObjects());
        assertEquals(0, list7.get(0).getSize());

    }

}
