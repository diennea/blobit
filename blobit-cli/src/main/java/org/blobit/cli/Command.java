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

import com.beust.jcommander.Parameter;
import herddb.jdbc.HerdDBDataSource;
import org.blobit.core.api.Configuration;
import org.blobit.core.api.ObjectManager;
import org.blobit.core.api.ObjectManagerFactory;

/**
 * Base class for commands.
 * @author eolivelli
 */
public abstract class Command extends AbstractCommand {

    @Parameter(names = "--zk", description = "ZooKeeper connection string")
    String zk = "localhost:2181";

    @Parameter(names = "-v", description = "Provide verbose messages")
    boolean verbose;


    public Command(CommandContext cm) {
        super(cm);
    }


    protected void modifyConfiguration(Configuration clientConfig) {

    }

    public void doWithClient(ProcedureWithClient procedure) throws Exception {
        Configuration clientConfig = new Configuration();
        clientConfig.setZookeeperUrl(zk);
        clientConfig.setUseTablespaces(true);
        modifyConfiguration(clientConfig);


        try (final HerdDBDataSource ds = new HerdDBDataSource();) {
            ds.setUrl("jdbc:herddb:zookeeper:" + zk + "/herd");
            try (final ObjectManager client = ObjectManagerFactory.createObjectManager(clientConfig, ds)) {
                procedure.accept(client);
            }
        }
    }

    public abstract void execute() throws Exception;

    @FunctionalInterface
    public interface ProcedureWithClient {

        void accept(ObjectManager client) throws Exception;
    }

}
