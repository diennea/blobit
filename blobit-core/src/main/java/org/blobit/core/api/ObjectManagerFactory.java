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
package org.blobit.core.api;

import javax.sql.DataSource;
import org.blobit.core.cluster.BookKeeperBlobManager;
import org.blobit.core.cluster.HerdDBMetadataStorageManager;
import org.blobit.core.mem.LocalManager;

/**
 * Creates DataManager instances
 *
 * @author enrico.olivelli
 */
public class ObjectManagerFactory {

    /**
     * Create a new Database. You need to close it in order to release resources
     *
     * @param configuration
     * @param datasource
     * @return
     * @throws ObjectManagerException
     * @see ObjectManager#close()
     */
    public static ObjectManager createObjectManager(Configuration configuration, DataSource datasource) throws ObjectManagerException {

        ObjectManager result;
        switch (configuration.getType()) {
            case Configuration.TYPE_BOOKKEEPER: {
                HerdDBMetadataStorageManager metadataStorageManager = new HerdDBMetadataStorageManager(datasource,
                    configuration);
                metadataStorageManager.init();
                result = new BookKeeperBlobManager(configuration, metadataStorageManager);
                break;
            }
            case Configuration.TYPE_MEM: {
                result = new LocalManager();
                break;
            }
            default:
                throw new RuntimeException("bad type " + configuration.getType());
        }
        result.start();
        return result;
    }
}
