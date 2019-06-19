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
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicLong;
import org.blobit.core.api.BucketHandle;
import org.blobit.core.api.Configuration;
import org.blobit.core.api.ObjectManagerException;
import org.blobit.core.api.PutPromise;

/**
 *
 * @author eolivelli
 */
@Parameters(commandDescription = "Put a BLOB")
public class CommandAppend extends BucketCommand {
    private static PutPromise writeFile(File file, BucketHandle bucketHandle)
            throws IOException, InterruptedException, ObjectManagerException {
        try (InputStream ii = new BufferedInputStream(new FileInputStream(file))) {
            PutPromise put = bucketHandle
                    .put(null, file.length(), ii);
            System.out.println("PUT PROMISE: object id: '" + put.id + "'");
            return put;
        }
    }

    @Parameter(names = "--name", description = "Name of the blob, default to the same name of the file to write")
    public String name;

    @Parameter(names = "--checksum", description = "Create a checksum", arity = 1)
    public boolean checksum = true;

    @Parameter(names = "--deferred-sync", description = "Use DEFERRED_SYNC flag", arity = 1)
    public boolean deferredSync = false;

    @Parameter(names = "--in", description = "Local file or directory to write", required = true)
    public File file;

    @Parameter(names = "--max-entry-size", description = "Max extry size, in bytes, defaults to 65536")
    private int maxEntrySize = 65536;

    @Parameter(names = "--replication", description = "Replication factor, defaults to 1")
    public int replication = 1;

    public CommandAppend(CommandContext main) {
        super(main);
    }

    @Override
    protected void modifyConfiguration(Configuration clientConfig) {
        clientConfig.setMaxEntrySize(maxEntrySize);
        clientConfig.setReplicationFactor(replication);
        clientConfig.setEnableCheckSum(checksum);
        clientConfig.setDeferredSync(deferredSync);
    }

    @Override
    public void execute() throws Exception {
        long _start = System.currentTimeMillis();
        AtomicLong totalBytes = new AtomicLong();
        if (file.isFile()) {
            System.out.println("APPEND BUCKET '" + bucket + "' NAME '" + name + "' " + file.length()
                    + " bytes (maxEntrySize " + maxEntrySize + " bytes, replicationFactor: " + replication
                    + ", checksum:" + checksum + " deferredSync:" + deferredSync + ")");
        } else {
            throw new IOException("File " + file + " does not exists or is not a file");
        }

        doWithClient(client -> {
            if (name == null || name.isEmpty()) {
                name = file.getName();
            }

            BucketHandle bucketHandle = client.getBucket(bucket);
            PutPromise promise = writeFile(file, bucketHandle);
            promise.get();
            bucketHandle.append(promise.id, name);
            long _stop = System.currentTimeMillis();
            double speed = (totalBytes.get() * 1000) / (1024 * 1024.0 * (_stop - _start));
            System.out.println("1 OBJECT " + (totalBytes.get() / (1024 * 1024)) + " MBs WRITTEN SUCCESSFULLY, " + speed
                    + " MB/s");
        });
    }


}
