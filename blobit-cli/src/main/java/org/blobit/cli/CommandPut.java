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
import com.beust.jcommander.Parameters;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import org.blobit.core.api.Configuration;
import org.blobit.core.api.PutPromise;

/**
 *
 * @author eolivelli
 */
@Parameters(commandDescription = "Put a BLOB")
public class CommandPut extends BucketCommand {

    @Parameter(names = "--name", description = "Name of the blob", required = true)
    public String name;

    @Parameter(names = "--in", description = "File to read", required = true)
    public File file;

    @Parameter(names = "-mes", description = "Max extry size, in bytes, defaults to 65536")
    private int maxEntrySize = 65536;

    @Parameter(names = "-r", description = "Replication factor, defaults to 1")
    private int replicationFactor = 1;

    public CommandPut(CommandContext main) {
        super(main);
    }

    @Override
    protected void modifyConfiguration(Configuration clientConfig) {
        clientConfig.setMaxEntrySize(maxEntrySize);
        clientConfig.setReplicationFactor(replicationFactor);
    }

    @Override
    public void execute() throws Exception {
        long _start = System.currentTimeMillis();
        System.out.println("PUT BUCKET '" + bucket + "' NAME '" + name + "' " + file.length() + " bytes (maxEntrySize " + maxEntrySize + " butes, replicationFactor: " + replicationFactor + ")");
        doWithClient(client -> {
            try (InputStream ii = new BufferedInputStream(new FileInputStream(file))) {
                PutPromise put = client.getBucket(bucket)
                        .put(name, file.length(), ii);
                System.out.println("PUT PROMISE: object id: '" + put.id + "'");
                put.get();
                long _stop = System.currentTimeMillis();
                double speed = (file.length() * 1000 * 60 * 60.0) / (1024 * 1024.0 * (_stop - _start));
                System.out.println("OBJECT WRITTEN SUCCESSFULLY, " + speed + " MB/h");
            }
        });
    }

}
