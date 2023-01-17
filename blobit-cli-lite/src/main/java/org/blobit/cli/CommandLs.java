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
import herddb.utils.IntHolder;
import herddb.utils.LongHolder;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.blobit.core.api.LocationInfo;
import org.blobit.core.api.LocationInfo.ServerInfo;
import org.blobit.core.api.NamedObjectMetadata;
import org.blobit.core.api.ObjectManagerException;
import org.blobit.core.api.ObjectMetadata;
import org.blobit.core.filters.NamedObjectFilters;

/**
 * List named objects
 */
@Parameters(commandDescription = "List named objects")
public class CommandLs extends BucketCommand {

    @Parameter(names = "--starts-with", description = "Beginning of the name of the blob")
    public String prefix = "";

    @Parameter(names = "--objects-info", description = "Detailed objects info")
    public boolean objectsInfo = false;

    @Parameter(names = "--location-info", description = "Location info")
    public boolean locationInfo = false;

    public CommandLs(CommandContext main) {
        super(main);
    }

    @Override
    public void execute() throws Exception {
        long _start = System.currentTimeMillis();
        System.out.println("LS BUCKET '" + bucket + "' PREFIX '" + prefix + "'");
        doWithClient(client -> {
            IntHolder count = new IntHolder();
            LongHolder totalSize = new LongHolder();
            client.getBucket(bucket)
                    .listByName(NamedObjectFilters.nameStartsWith(prefix), (NamedObjectMetadata namedmetadata) -> {
                        count.value++;
                        totalSize.value += namedmetadata.getSize();
                        if (namedmetadata.getNumObjects() == 1) {
                            System.out.println(namedmetadata.getName() + "," + namedmetadata.getSize() + " bytes");
                        } else {
                            System.out.println(
                                    namedmetadata.getName() + "," + namedmetadata.getSize()
                                    + " bytes (" + namedmetadata.getNumObjects() + " objects)");
                        }
                        if (objectsInfo) {
                            for (int i = 0; i < namedmetadata.getNumObjects(); i++) {
                                ObjectMetadata metadata = namedmetadata.getObject(i);
                                System.out.println("OBJECT ID: " + metadata.id + "," + metadata.size + " bytes");
                                if (locationInfo) {
                                    try {
                                        LocationInfo lInfo = client.getBucket(bucket)
                                                .getLocationInfo(metadata.id).get();
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
                                    } catch (ObjectManagerException | InterruptedException | ExecutionException ex) {
                                        throw new RuntimeException(ex);
                                    }
                                }
                            }
                        }
                        return true;
                    });
            System.out.println("TOTAL: " + count.value + " named objects, " + totalSize.value + " bytes");
        });
    }

}
