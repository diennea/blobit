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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.blobit.core.api.BucketHandle;
import org.blobit.core.api.Configuration;
import org.blobit.core.api.ObjectManagerException;
import org.blobit.core.api.PutOptions;
import org.blobit.core.api.PutPromise;

/**
 *
 * @author eolivelli
 */
@Parameters(commandDescription = "Put a named object")
public class CommandPut extends BucketCommand {

    private void writeOrScan(File theFile, String name, BucketHandle bucketHandle,
                             AtomicInteger totalFiles, AtomicLong totalBytes, String prefixPath,
                             List<PutPromise> results) throws IOException,
            InterruptedException, ObjectManagerException {
        if (theFile.isDirectory() && name == null) {
            File[] children = theFile.listFiles();
            if (children != null) {
                String fileName = theFile.getName();
                if (!fileName.equals(".")) {
                    prefixPath = prefixPath + fileName + "/";
                }
                for (File child : children) {
                    writeOrScan(child, null, bucketHandle, totalFiles, totalBytes, prefixPath, results);
                }
            }
        } else {
            writeFile(theFile, name, bucketHandle, totalFiles, totalBytes, prefixPath, results);
        }
    }

    private void writeFile(File file, String name, BucketHandle bucketHandle,
                           AtomicInteger totalCount, AtomicLong totalWritten, String prefixPath,
                           List<PutPromise> results)
            throws IOException, InterruptedException, ObjectManagerException {
        if (name == null || name.isEmpty()) {
            name = prefixPath + file.getName();
        }
        totalCount.incrementAndGet();
        try (InputStream ii = new BufferedInputStream(new FileInputStream(file))) {
            PutPromise put = bucketHandle
                    .put(name, file.length(), ii, PutOptions.builder()
                            .overwrite(overwrite)
                            .append(append)
                            .build());
            System.out.println("PUT PROMISE: object id: '" + put.id + "' name: '" + name + "'");
            results.add(put);
            totalWritten.addAndGet(file.length());
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

    @Parameter(names = "--overwrite", description = "Overwrite in case of existing named object")
    public boolean overwrite = false;

    @Parameter(names = "--append", description = "Append in case of existing named object")
    public boolean append = false;

    @Parameter(names = "--replication", description = "Replication factor, defaults to 1")
    public int replication = 1;

    public CommandPut(CommandContext main) {
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
        AtomicInteger totalFiles = new AtomicInteger();

        if (file.isFile()) {
            if (name == null) {
                name = file.getName();
            }
            System.out.println("PUT BUCKET '" + bucket + "' NAME '" + name + "' " + file.length()
                    + " bytes (maxEntrySize " + maxEntrySize + " bytes, replicationFactor: " + replication
                    + ", checksum:" + checksum + " deferredSync:" + deferredSync + ")");
        } else if (file.isDirectory()) {
            System.out.println("PUT BUCKET '" + bucket + "' DIRECTORY '" + file + "' (maxEntrySize " + maxEntrySize
                    + " bytes, replicationFactor: " + replication + ", checksum:" + checksum + " deferredSync:"
                    + deferredSync + ")");
        } else {
            throw new IOException("File " + file + " does not exists");
        }
        doWithClient(client -> {
            BucketHandle bucketHandle = client.getBucket(bucket);
            File theFile = file;
            List<PutPromise> results = new ArrayList<>();
            writeOrScan(theFile, name, bucketHandle, totalFiles, totalBytes, "/", results);
            // really wait for all of the operations to finish
            for (PutPromise pp : results) {
                pp.get();
            }
            long _stop = System.currentTimeMillis();
            double speed = (totalBytes.get() * 1000) / (1024 * 1024.0 * (_stop - _start));
            System.out.println(totalFiles + " OBJECTs " + (totalBytes.get() / (1024 * 1024))
                    + " MBs WRITTEN SUCCESSFULLY, " + speed + " MB/s");
        });
    }

}
