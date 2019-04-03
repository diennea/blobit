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

import com.beust.jcommander.Parameters;
import java.util.concurrent.atomic.AtomicInteger;
import org.blobit.core.api.BucketConfiguration;

/**
 * Delete a bucket
 *
 * @author eolivelli
 */
@Parameters(commandDescription = "Deletes a bucket")
public class CommandListBuckets extends Command {

    public CommandListBuckets(CommandContext main) {
        super(main);
    }

    @Override
    public void execute() throws Exception {
        System.out.println("LIST BUCKETS:");
        AtomicInteger count = new AtomicInteger();
        doWithClient(client -> {
            client.listBuckets((md) -> {
                System.out.println("BUCKET '" + md.getBucketId() + "', uuid '" + md.getUuid() + "' tablespace '" + md.getTableSpaceName() + "'");
                count.incrementAndGet();
            });
        });
        System.out.println("FOUND " + count + " BUCKETS");

    }

}
