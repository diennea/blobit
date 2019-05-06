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
import java.util.List;
import java.util.stream.Collectors;
import org.blobit.core.api.LocationInfo;
import org.blobit.core.api.LocationInfo.ServerInfo;
import org.blobit.core.api.ObjectMetadata;

/**
 *
 * @author eolivelli
 */
@Parameters(commandDescription = "Get info about a BLOB")
public class CommandStat extends BucketCommand {

    @Parameter(names = "--name", description = "Name of the blob", required = true)
    public String name;

    @Parameter(names = "--location-info", description = "Location info")
    public boolean locationInfo = false;

    public CommandStat(CommandContext main) {
        super(main);
    }

    @Override
    public void execute() throws Exception {
        long _start = System.currentTimeMillis();
        System.out.println("STAT BUCKET '" + bucket + "' NAME '" + name);
        doWithClient(client -> {
            ObjectMetadata metadata = client.getBucket(bucket)
                    .statByName(name);
            if (metadata == null) {
                System.out.println("OBJECT NOT FOUND NAME:" + name);
                return;
            }
            System.out.println("OBJECT ID: " + metadata.id);
            System.out.println("OBJECT SIZE: " + metadata.size + " bytes");
            if (locationInfo) {
                LocationInfo lInfo = client.getBucket(bucket).getLocationInfo(metadata.id).get();
                List<Long> segments = lInfo.getSegmentsStartOffsets();
                System.out.println("LOCATION INFO:");
                System.out.println("NUM SEGMENTS: " + segments.size());
                for (Long offset : segments) {
                    System.out.println("OFFSET " + offset + ": LOCATED AT: "
                            + lInfo.getServersAtPosition(offset)
                                    .stream()
                                    .map(ServerInfo::getAddress)
                                    .collect(Collectors.joining(",")));
                }
            }
        });
    }

}
