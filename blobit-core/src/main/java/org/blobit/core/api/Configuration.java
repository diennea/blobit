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

import java.util.Collection;
import java.util.Properties;

/**
 * Configuration of the DataManager
 *
 * @author enrico.olivelli
 */
public class Configuration {

    private final Properties properties;

    public static final String MANAGER_TYPE = "blobmanager.type";

    public static final String TYPE_BOOKKEEPER = "bookkeeper";
    public static final String TYPE_MEM = "mem";
    public static final String MANAGER_TYPE_DEFAULT = TYPE_MEM;

    public static final String REPLICATION_FACTOR = "replication.factor";
    public static final int REPLICATION_FACTOR_DEFAULT = 1;

    public static final String BUCKETS_TABLESPACE = "buckets.tablespace";
    public static final String BUCKETS_TABLESPACE_DEFAULT = "BUCKETS";

    public static final String USE_TABLESPACES = "usetablespaces";
    public static final String USE_TABLESPACES_DEFAULT = "true";

    public static final String MANAGE_TABLESPACES = "usetablespaces";
    public static final String MANAGE_TABLESPACES_DEFAULT = "true";

    public static final String MAX_BYTES_PER_LEDGER = "max.bytes.per.ledger";
    public static final int MAX_BYTES_PER_LEDGER_DEFAULT = 1024 * 1024 * 2;

    public static final String MAX_ENTRY_SIZE = "max.entry.size";
    public static final int MAX_ENTRY_SIZE_DEFAULT = 64 * 1024;

    public static final String CUNCURRENT_WRITERS = "concurrent.writers";
    public static final int CUNCURRENT_WRITERS_DEFAULT = 1;

    public static final String MAX_READERS = "max.readers";
    public static final int MAX_READERS_DEFAULT = 100;

    public static final String ENABLE_CHECKSUM = "enable.checksum";
    public static final boolean ENABLE_CHECKSUM_DEFAULT = true;

    public static final String DEFERRED_SYNC = "deferred.sync";
    public static final boolean DEFERRED_SYNC_DEFAULT = false;

    public static final String ZOOKEEPER_URL = "zookeeper.url";
    public static final String ZOOKEEPER_URL_DEFAULT = "localhost:2181";

    public static final String BOOKKEEPER_ZK_LEDGERS_ROOT_PATH =
            "client.bookkeeper.zk.ledgers.root.path";
    public static final String BOOKKEEPER_ZK_LEDGERS_ROOT_PATH_DEFAULT =
            "/ledgers";

    public Configuration() {
        this.properties = new Properties();
    }

    public Configuration(Properties properties) {
        this.properties = new Properties();
        this.properties.putAll(properties);
    }

    public Properties toProperties() {
        Properties res = new Properties();
        res.putAll(properties);
        return res;
    }

    public Configuration setProperty(String key, Object value) {
        properties.put(key, value);
        return this;
    }

    public String getProperty(String key, String defaultValue) {
        return properties.getProperty(key, defaultValue);
    }

    public String getType() {
        return properties.getProperty(MANAGER_TYPE, TYPE_BOOKKEEPER);
    }

    public Configuration setType(String type) {
        properties.put(MANAGER_TYPE, type);
        return this;
    }

    public boolean isUseTablespaces() {
        return Boolean.parseBoolean(properties.getProperty(USE_TABLESPACES,
                USE_TABLESPACES_DEFAULT));
    }

    public boolean isManageTablespaces() {
        return Boolean.parseBoolean(properties.getProperty(MANAGE_TABLESPACES,
                MANAGE_TABLESPACES_DEFAULT));
    }

    public Configuration setUseTablespaces(boolean value) {
        properties.put(USE_TABLESPACES, value);
        return this;
    }

    public Configuration setZookeeperUrl(String zkUrl) {
        properties.put(ZOOKEEPER_URL, zkUrl);
        return this;
    }

    public String getZookkeeperUrl() {
        return properties.getProperty(ZOOKEEPER_URL, ZOOKEEPER_URL_DEFAULT);
    }

    public Configuration setBucketsTableSpace(String value) {
        properties.put(BUCKETS_TABLESPACE, value);
        return this;
    }

    public String getBucketsTableSpace() {
        return properties.getProperty(BUCKETS_TABLESPACE,
                BUCKETS_TABLESPACE_DEFAULT);
    }

    public Configuration setReplicationFactor(int factor) {
        properties.put(REPLICATION_FACTOR, factor + "");
        return this;
    }

    public int getReplicationFactor() {
        return Integer.parseInt(properties.getProperty(REPLICATION_FACTOR,
                REPLICATION_FACTOR_DEFAULT + ""));
    }

    public Configuration setMaxBytesPerLedger(long value) {
        properties.put(MAX_BYTES_PER_LEDGER, value + "");
        return this;
    }

    public long getMaxBytesPerLedger() {
        return Long.parseLong(properties.getProperty(MAX_BYTES_PER_LEDGER,
                MAX_BYTES_PER_LEDGER_DEFAULT + ""));
    }

    public Configuration setMaxEntrySize(int value) {
        properties.put(MAX_ENTRY_SIZE, value + "");
        return this;
    }

    public int getMaxEntrySize() {
        return Integer.parseInt(properties.getProperty(MAX_ENTRY_SIZE,
                MAX_ENTRY_SIZE_DEFAULT + ""));
    }

    public Configuration setConcurrentWriters(int v) {
        properties.put(CUNCURRENT_WRITERS, v + "");
        return this;
    }

    public Configuration setConcurrentReaders(int v) {
        properties.put(MAX_READERS, v + "");
        return this;
    }

    public int getConcurrentWriters() {
        return Integer.parseInt(properties.getProperty(CUNCURRENT_WRITERS,
                CUNCURRENT_WRITERS_DEFAULT + ""));
    }

    public int getMaxReaders() {
        return Integer.parseInt(properties.getProperty(MAX_READERS,
                MAX_READERS_DEFAULT + ""));
    }

    public boolean isEnableChecksum() {
        return Boolean.parseBoolean(properties.getProperty(ENABLE_CHECKSUM,
                ENABLE_CHECKSUM_DEFAULT + ""));
    }

    public Configuration setEnableCheckSum(boolean value) {
        properties.put(ENABLE_CHECKSUM, value);
        return this;
    }

    public boolean isDeferredSync() {
        return Boolean.parseBoolean(properties.getProperty(DEFERRED_SYNC,
                DEFERRED_SYNC_DEFAULT + ""));
    }

    public Configuration setDeferredSync(boolean value) {
        properties.put(DEFERRED_SYNC, value);
        return this;
    }

    public Collection<String> keys() {
        return (Collection<String>) (Collection<?>) properties.keySet();
    }

    public Object getProperty(String key) {
        return properties.get(key);
    }

}
