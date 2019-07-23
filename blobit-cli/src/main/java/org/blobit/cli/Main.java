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
import com.beust.jcommander.ParameterException;

/**
 * Basic CLI
 */
public class Main {

    public static void main(String... args) throws Exception {
        AbstractCommand command = Main.parseCommandLine(args);
        try {
            command.execute();
        } catch (Exception err) {
            if (command instanceof Command && ((Command) command).verbose) {
                err.printStackTrace();
            } else {
                System.err.println("An error occurred: " + err.getMessage());
            }
            System.exit(1);
        }
    }

    public static <T extends AbstractCommand> T parseCommandLine(String[] args) throws Exception {
        CommandContext cm = new CommandContext();
        JCommander jc = JCommander.newBuilder()
                .programName("blobit.sh")
                .addObject(cm)
                .addCommand("createbucket", new CommandCreateBucket(cm))
                .addCommand("deletebucket", new CommandDeleteBucket(cm))
                .addCommand("gcbucket", new CommandGcBucket(cm))
                .addCommand("listbuckets", new CommandListBuckets(cm))
                .addCommand("put", new CommandPut(cm))
                .addCommand("get", new CommandGet(cm))
                .addCommand("stat", new CommandStat(cm))
                .addCommand("help", new CommandHelp(cm))
                .build();
        cm.jCommander = jc;
        try {
            jc.parse(args);
        } catch (ParameterException err) {
            System.out.println("Error: " + err.getMessage());
            return (T) new CommandHelp(cm);
        }
        if (jc.getParsedCommand() == null) {
            return (T) new CommandHelp(cm);
        }
        return (T) jc.getCommands().get(jc.getParsedCommand()).getObjects().get(0);
    }
}
