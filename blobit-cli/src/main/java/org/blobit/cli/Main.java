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
package org.blobit.cli;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import herddb.jdbc.HerdDBDataSource;
import org.blobit.core.api.BucketConfiguration;
import org.blobit.core.api.Configuration;
import org.blobit.core.api.ObjectManager;
import org.blobit.core.api.ObjectManagerFactory;

/**
 * Basic CLI
 */
public class Main {

    @Parameters(separators = "=", commandDescription = "Creates a bucket")
    private static class CommandCreateBucket {

        @Parameter(names = "--bucket", description = "Name of the bucket", required = true)
        private String name;

        public void execute() throws Exception {
            System.out.println("CREATE BUCKET '" + name + "'");
            Configuration clientConfig = new Configuration();
            clientConfig.setZookeeperUrl(cm.zk);
            try (HerdDBDataSource ds = new HerdDBDataSource();
                    ObjectManager client = ObjectManagerFactory.createObjectManager(clientConfig, ds)) {
                ds.setUrl("jdbc:herddb:zookeeper:" + cm.zk + "/herd");
                client.createBucket(name, name, BucketConfiguration.DEFAULT);
            }
        }
    }

    @Parameters(separators = "=")
    private static class CommandMain {

        @Parameter(names = "--zk", description = "ZooKeeper connection string", required = true)
        String zk = "localhost:2181";

    }

    static CommandMain cm = new CommandMain();
    static CommandCreateBucket createbucket = new CommandCreateBucket();

    public static void main(String... args) throws Exception {

        JCommander jc = JCommander.newBuilder()
                .addObject(cm)
                .addCommand("createbucket", createbucket)
                .build();

        jc.parse(args);

        switch (jc.getParsedCommand() + "") {
            case "createbucket":
                createbucket.execute();
                break;
            default:
                jc.usage();
                break;
        }
    }
}
