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
import java.lang.reflect.Field;
import java.util.Properties;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.QuorumPeerMain;

/**
 * Simple ZookKeeper Server, with SASL support
 *
 * @author enrico.olivelli
 */
public class TestingZookeeperServerEmbedded implements AutoCloseable {

    static {
        System.setProperty("zookeeper.admin.enableServer", "false");
    }

    QuorumPeerConfig config;
    QuorumPeerMain maincluster;
    ZooKeeperServerMain mainsingle;
    Thread thread;
    ServerCnxnFactory cnxnFactory;

    public TestingZookeeperServerEmbedded(int clientPort, File baseDir) throws Exception {
        Properties p = new Properties();
        String host = "localhost";
        String dataDir = "data";
        File dir = new File(baseDir, dataDir);
        p.setProperty("syncEnabled", "false");
        p.setProperty("dataDir", dir.getAbsolutePath());
        p.setProperty("clientPort", clientPort + "");
        p.setProperty("authProvider.1", "org.apache.zookeeper.server.auth.SASLAuthenticationProvider");
        p.setProperty("kerberos.removeHostFromPrincipal", "true");
        p.setProperty("kerberos.removeRealmFromPrincipal", "true");

        config = new QuorumPeerConfig();

        config.parseProperties(p);

    }

    public void start() throws Exception {

        mainsingle = new ZooKeeperServerMain();

        thread = new Thread("zkservermainrunner") {
            @Override
            public void run() {
                try {
                    ServerConfig cc = new ServerConfig();
                    cc.readFrom(config);
                    mainsingle.runFromConfig(cc);
                    System.out.println("ZK server died");
                } catch (Throwable t) {
                    t.printStackTrace();
                }
            }
        };
        thread.start();

        this.cnxnFactory = getServerConnectionFactory();
        if (cnxnFactory != null) {
            final ZooKeeperServer zkServer = getZooKeeperServer(cnxnFactory);
            if (zkServer != null) {
                synchronized (zkServer) {
                    if (!zkServer.isRunning()) {
                        zkServer.wait();
                    }
                }
            }
        }

    }

    private ServerCnxnFactory getServerConnectionFactory() throws Exception {
        Field cnxnFactoryField = ZooKeeperServerMain.class.getDeclaredField("cnxnFactory");
        cnxnFactoryField.setAccessible(true);
        ServerCnxnFactory cnxnFactory;

        // Wait until the cnxnFactory field is non-null or up to 1s, whichever comes first.
        long startTime = System.currentTimeMillis();
        do {
            cnxnFactory = (ServerCnxnFactory) cnxnFactoryField.get(mainsingle);
        } while ((cnxnFactory == null) && ((System.currentTimeMillis() - startTime) < 10000));

        return cnxnFactory;
    }

    private ZooKeeperServer getZooKeeperServer(ServerCnxnFactory cnxnFactory) throws Exception {
        Field zkServerField = ServerCnxnFactory.class.getDeclaredField("zkServer");
        zkServerField.setAccessible(true);
        ZooKeeperServer zkServer;

        // Wait until the zkServer field is non-null or up to 1s, whichever comes first.
        long startTime = System.currentTimeMillis();
        do {
            zkServer = (ZooKeeperServer) zkServerField.get(cnxnFactory);
        } while ((zkServer == null) && ((System.currentTimeMillis() - startTime) < 10000));

        return zkServer;
    }

    @Override
    public void close() {
        if (cnxnFactory != null) {
            cnxnFactory.shutdown();
        }

        if (thread != null) {
            thread.interrupt();
            try {
                thread.join();
            } catch (InterruptedException ex) {
            }
        }
    }
}
