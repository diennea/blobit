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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

public class MainTest {

    @Test
    public void testMain() throws Exception {
        CommandCreateBucket cl = Main.parseCommandLine(new String[]{"createbucket", "--bucket", "foo"});
        System.out.println("res:" + cl);
        assertEquals("foo", cl.bucket);
        assertEquals("localhost:2181", cl.zk);
    }

    @Test
    public void testSetZk() throws Exception {
        CommandCreateBucket cl = Main.parseCommandLine(new String[]{"createbucket", "--bucket", "foo",
            "--zk", "localhost:1234"});
        System.out.println("res:" + cl);
        assertEquals("foo", cl.bucket);
        assertEquals("localhost:1234", cl.zk);
    }

    @Test
    public void testNoCommand() throws Exception {
        CommandHelp cl = Main.parseCommandLine(new String[]{});
        System.out.println("res:" + cl);
        assertTrue(cl instanceof CommandHelp);
    }

}
