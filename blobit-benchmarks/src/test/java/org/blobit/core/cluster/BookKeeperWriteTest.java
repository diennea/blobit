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

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.LongAdder;

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.WriteHandle;

/**
 *
 * @author enrico.olivelli
 */
public class BookKeeperWriteTest {

    private static final byte[] TEST_DATA = new byte[35 * 1024];
    private static final int TEST_SIZE = 1000;
    private static final int TEST_ITERATIONS = 10;

    @Rule
    public final TemporaryFolder tmp = new TemporaryFolder(new File("target").getAbsoluteFile());

    @Test
    public void test() throws Exception {
        try (ZKTestEnv env = new ZKTestEnv(tmp.newFolder("zk").toPath());) {
            env.startBookie();
            ClientConfiguration clientConfiguration = new ClientConfiguration();

            // this is like (sleep(1)) in the loop below
            clientConfiguration.setThrottleValue(0);

            clientConfiguration.setZkServers(env.getAddress());

            // this reduce most of GC
            clientConfiguration.setUseV2WireProtocol(true);

            ByteBuf data = Unpooled.directBuffer(TEST_DATA.length);
            data.writeBytes(TEST_DATA);

            try (BookKeeper bk = new BookKeeper(clientConfiguration);) {

                for (int j = 0; j < TEST_ITERATIONS; j++) {
                    try (
                            WriteHandle lh = bk.
                                    newCreateLedgerOp()
                                    .withAckQuorumSize(1)
                                    .withEnsembleSize(1)
                                    .withWriteQuorumSize(1)
                                    .withPassword(new byte[0])
                                    .withDigestType(DigestType.CRC32C)
                                    .execute()
                                    .get()) {
                                LongAdder totalTime = new LongAdder();
                                long _start = System.currentTimeMillis();
                                Collection<CompletableFuture> batch = new ConcurrentLinkedQueue<>();
                                for (int i = 0; i < TEST_SIZE; i++) {
                                    long start = System.currentTimeMillis();

                                    CompletableFuture<Long> cf = lh.appendAsync(data);
                                    cf.handle((id, error) -> {
                                        totalTime.add(System.currentTimeMillis() - start);
                                        return null;
                                    });
                                    batch.add(cf);

//                          Thread.sleep(1);
                                }
                                assertEquals(TEST_SIZE, batch.size());
                                for (CompletableFuture f : batch) {
                                    f.get();
                                }
                                long _stop = System.currentTimeMillis();
                                double delta = _stop - _start;
                                System.out.printf("#" + j + " Wall clock time: " + delta + " ms, "
                                        // + "total callbacks time: " + totalTime.sum() + " ms, "
                                        + "size %.3f MB -> %.2f ms per entry (latency),"
                                        + "%.1f ms per entry (throughput) %.1f MB/s throughput%n",
                                        (TEST_DATA.length / (1024 * 1024d)),
                                        (totalTime.sum() * 1d / TEST_SIZE),
                                        (delta / TEST_SIZE),
                                        ((((TEST_SIZE * TEST_DATA.length) / (1024 * 1024d))) / (delta / 1000d)));
                            }
                }

            }
        }
    }
}
