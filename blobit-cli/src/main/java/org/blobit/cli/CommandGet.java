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
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import org.blobit.core.api.DownloadPromise;
import org.blobit.core.api.ObjectMetadata;

/**
 *
 * @author eolivelli
 */
@Parameters(commandDescription = "Get a BLOB")
public class CommandGet extends BucketCommand {

    @Parameter(names = "--name", description = "Name of the blob", required = true)
    public String name;

    @Parameter(names = "--out", description = "File to write to", required = true)
    public File file;

    public CommandGet(CommandContext main) {
        super(main);
    }

    @Override
    public void execute() throws Exception {
        long _start = System.currentTimeMillis();
        System.out.println("GET BUCKET '" + bucket + "' NAME '" + name + "' to " + file.getAbsolutePath());
        if (file.exists()) {
            throw new Exception("File " + file.getAbsolutePath() + " already exists");
        }
        doWithClient(client -> {
            try (OutputStream ii = new BufferedOutputStream(new FileOutputStream(file))) {
                DownloadPromise stat = client.getBucket(bucket)
                        .downloadByName(name, (l) -> {
                        }, ii, 0, -1);
                System.out.println("FOUND OBJECT ID:" + stat.id + ", size " + stat.length + " bytes");
                stat.get();
                long _stop = System.currentTimeMillis();
                double speed = (file.length() * 1000 * 60 * 60.0) / (1024 * 1024.0 * (_stop - _start));
                System.out.println("OBJECT DOWNLOADED SUCCESSFULLY, " + speed + " MB/h");

            }
        });
    }

}
