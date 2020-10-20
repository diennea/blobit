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

import static org.blobit.core.api.Configuration.BOOKKEEPER_ZK_LEDGERS_ROOT_PATH_DEFAULT;
import static org.blobit.core.util.TestUtils.NOOP;
import java.nio.file.Path;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.common.util.ReflectionUtils;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.BookieServiceInfo;
import org.apache.bookkeeper.meta.HierarchicalLedgerManagerFactory;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.stats.StatsProvider;
import org.blobit.core.util.TestUtils;

public class ZKTestEnv implements AutoCloseable {

    TestingZookeeperServerEmbedded zkServer;
    BookieServer bookie;
    Path path;

    public ZKTestEnv(Path path) throws Exception {
        zkServer = new TestingZookeeperServerEmbedded(1282, path.toFile());
        zkServer.start();
        this.path = path;
    }

    public void startBookie() throws Exception {
        if (bookie != null) {
            throw new Exception("bookie already started!");
        }
        ServerConfiguration conf = new ServerConfiguration();
        conf.setBookiePort(5621);
        conf.setUseHostNameAsBookieID(true);

        Path targetDir = path.resolve("bookie_data");
        conf.setZkServers("localhost:1282");
        conf.setLedgerDirNames(new String[]{targetDir.toAbsolutePath().
            toString()});
        conf.
                setLedgerManagerFactoryClass(
                        HierarchicalLedgerManagerFactory.class);
        conf.setZkLedgersRootPath(BOOKKEEPER_ZK_LEDGERS_ROOT_PATH_DEFAULT);
        int numJournals = 1;
        String[] journals = new String[numJournals];
        for (int i = 0; i < journals.length; i++) {
            Path jpath = targetDir.resolve("journal-" + i);
            journals[i] = jpath.toAbsolutePath().toString();
        }

        conf.setNumAddWorkerThreads(8);
        conf.setMaxPendingReadRequestPerThread(10000); // new in 4.6
        conf.setMaxPendingAddRequestPerThread(20000); // new in 4.6
        conf.setJournalSyncData(false); // new in 4.6, do not wait for fsync on journal
        conf.setJournalDirsName(journals);
        conf.setFlushInterval(100);
        conf.setJournalFlushWhenQueueEmpty(true);
//        conf.setSkipListSizeLimit(1024*1024*1024);
        conf.setProperty("journalMaxGroupWaitMSec", 10);
        //conf.setProperty("journalBufferedWritesThreshold", 1024);
        conf.setAutoRecoveryDaemonEnabled(false);
        conf.setEnableLocalTransport(true);
        conf.setAllowLoopback(true);

        BookKeeperAdmin.format(conf, false, false);
        Class<? extends StatsProvider> statsProviderClass =
                conf.getStatsProviderClass();
        StatsProvider statsProvider = ReflectionUtils.newInstance(
                statsProviderClass);
        statsProvider.start(conf);
        this.bookie = new BookieServer(conf, statsProvider.getStatsLogger(""), BookieServiceInfo.NO_INFO);
        this.bookie.start();
        TestUtils.waitForCondition(bookie::isRunning, NOOP, 100);
        System.out.println("[BOOKIE] started at " + bookie);
    }

    public BookieServer getBookie() {
        return bookie;
    }

    public String getAddress() {
        return "localhost:1282";
    }

    public int getTimeout() {
        return 40000;
    }

    public String getPath() {
        return "/test";
    }

    public void stopBookie() throws Exception {
        bookie.shutdown();
        bookie.join();
        bookie = null;
    }

    @Override
    public void close() throws Exception {
        try {
            if (bookie != null) {
                bookie.shutdown();
            }
        } catch (Throwable t) {
        }
        try {
            if (zkServer != null) {
                zkServer.close();
            }
        } catch (Throwable t) {
        }
    }

}
