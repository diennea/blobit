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
package blobit.server;

import java.nio.file.Path;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Server configuration
 *
 */
public final class ServerConfiguration {

    private final Properties properties;

    public static final String PROPERTY_BASEDIR = "server.base.dir";
    public static final String PROPERTY_BASEDIR_DEFAULT = "dbdata";
    public static final String PROPERTY_DATADIR = "server.data.dir";
    public static final String PROPERTY_DATADIR_DEFAULT = "data";
    public static final String PROPERTY_LOGDIR = "server.log.dir";
    public static final String PROPERTY_LOGDIR_DEFAULT = "txlog";
    public static final String PROPERTY_TMPDIR = "server.tmp.dir";
    public static final String PROPERTY_TMPDIR_DEFAULT = "tmp";
    public static final String PROPERTY_METADATADIR = "server.metadata.dir";
    public static final String PROPERTY_METADATADIR_DEFAULT = "metadata";
    public static final String PROPERTY_ADVERTISED_HOST = "server.advertised.host";
    public static final String PROPERTY_ADVERTISED_PORT = "server.advertised.port";
    public static final String PROPERTY_HOST = "server.host";
    public static final String PROPERTY_HOST_AUTODISCOVERY = "";
    public static final String PROPERTY_HOST_DEFAULT = "localhost";
    public static final String PROPERTY_PORT = "server.port";
    public static final String PROPERTY_SSL = "server.ssl";

    public static final String PROPERTY_ZOOKEEPER_ADDRESS = "server.zookeeper.address";
    public static final String PROPERTY_ZOOKEEPER_SESSIONTIMEOUT = "server.zookeeper.session.timeout";
    public static final String PROPERTY_ZOOKEEPER_PATH = "server.zookeeper.path";

    public static final String PROPERTY_BOOKKEEPER_START = "server.bookkeeper.start";
    public static final boolean PROPERTY_BOOKKEEPER_START_DEFAULT = false;

    public static final String PROPERTY_BOOKKEEPER_BOOKIE_PORT = "server.bookkeeper.port";
    public static final int PROPERTY_BOOKKEEPER_BOOKIE_PORT_DEFAULT = 3181;

    public static final String PROPERTY_BOOKKEEPER_ZK_LEDGERS_ROOT_PATH = "server.bookkeeper.zk.ledgers.root.path";
    public static final String PROPERTY_BOOKKEEPER_ZK_LEDGERS_ROOT_PATH_DEFAULT = "/blobit-bk";

    public static final String PROPERTY_ZOOKEEPER_ADDRESS_DEFAULT = "localhost:2181";

    public static final int PROPERTY_PORT_DEFAULT = 2183;
    public static final int PROPERTY_ZOOKEEPER_SESSIONTIMEOUT_DEFAULT = 40000;

    public ServerConfiguration(Properties properties) {
        this.properties = new Properties();
        this.properties.putAll(properties);
    }

    public ServerConfiguration(Path baseDir) {
        this();
        set(PROPERTY_BASEDIR, baseDir.toAbsolutePath());
    }

    /**
     * Copy configuration
     *
     * @return
     */
    public ServerConfiguration copy() {
        Properties copy = new Properties();
        copy.putAll(this.properties);
        return new ServerConfiguration(copy);
    }

    public ServerConfiguration() {
        this.properties = new Properties();
    }

    public boolean getBoolean(String key, boolean defaultValue) {
        final String value = this.properties.getProperty(key);

        if (value == null || value.isEmpty()) {
            return defaultValue;
        }

        return Boolean.parseBoolean(value);
    }

    public int getInt(String key, int defaultValue) {
        final String value = this.properties.getProperty(key);

        if (value == null || value.isEmpty()) {
            return defaultValue;
        }

        return Integer.parseInt(value);
    }

    public long getLong(String key, long defaultValue) {
        final String value = this.properties.getProperty(key);

        if (value == null || value.isEmpty()) {
            return defaultValue;
        }

        return Long.parseLong(value);
    }

    public double getDouble(String key, double defaultValue) {
        final String value = this.properties.getProperty(key);

        if (value == null || value.isEmpty()) {
            return defaultValue;
        }

        return Double.parseDouble(value);
    }

    public String getString(String key, String defaultValue) {
        return this.properties.getProperty(key, defaultValue);
    }

    public ServerConfiguration set(String key, Object value) {
        if (value == null) {
            this.properties.remove(key);
        } else {
            this.properties.setProperty(key, value + "");
        }
        return this;
    }

    @Override
    public String toString() {
        return "ServerConfiguration{" + "properties=" + properties + '}';
    }

    private static final Logger LOG = Logger.getLogger(ServerConfiguration.class.getName());

    public List<String> keys() {
        return properties.keySet().stream().map(Object::toString).sorted().collect(Collectors.toList());
    }

}
