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
package org.blobit.core.cluster;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import herddb.model.TableSpace;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.sql.DataSource;
import org.blobit.core.api.ObjectManagerException;
import org.blobit.core.api.ObjectMetadata;
import org.blobit.core.api.BucketConfiguration;
import org.blobit.core.api.BucketMetadata;
import org.blobit.core.api.Configuration;
import org.blobit.core.api.LedgerMetadata;
import org.blobit.core.api.MetadataManager;

/**
 * Stores metadata on HerdDB
 *
 * @author enrico.olivelli
 */
@SuppressFBWarnings("SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE")
public class HerdDBMetadataStorageManager implements MetadataManager {

    private final DataSource datasource;
    private final String bucketsTablespace;
    private final int bucketsTableSpacesReplicaCount;
    private final boolean useTablespaces;

    private static final String BUCKETS_TABLE = "BUCKETS";
    private static final String LEDGERS_TABLE = "LEDGERS";
    private static final String BLOBS_TABLE = "BLOBS";

    private static final String CREATE_BUCKETS_TABLE = "CREATE TABLE " + BUCKETS_TABLE + " ("
        + "bucketId string PRIMARY KEY,"
        + "tableSpaceName string,"
        + "configuration string)";

    private static final String SELECT_BUCKET
        = "SELECT bucketId,tableSpaceName, configuration "
        + "FROM " + BUCKETS_TABLE + " where bucketId = ?";

    private static final String LOAD_BUCKETS
        = "SELECT bucketId,tableSpaceName, configuration FROM " + BUCKETS_TABLE;

    private static final String INSERT_BUCKET
        = "INSERT INTO " + BUCKETS_TABLE + "(bucketId, tableSpaceName, configuration) values (?,?,?)";

    private String CREATE_TABLESPACE(String schema, int replicaCount) {
        return "CREATE TABLESPACE '" + schema + "','wait:60000','expectedreplicacount:" + replicaCount + "'";
    }

    private static final String CREATE_LEDGERS_TABLE
        = "CREATE TABLE " + LEDGERS_TABLE + "(ledgerid long PRIMARY KEY, creationDate timestamp, bucketId string)";

    private static final String CREATE_BLOBS_TABLE = "CREATE TABLE " + BLOBS_TABLE + "(ledgerid long, entryid long, "
        + "lastEntryId long, "
        + "size long, primary key (ledgerid, entryid))";

    private static final String REGISTER_BLOB = "INSERT "
        + "INTO " + BLOBS_TABLE + "(ledgerid, entryid, lastEntryId, size) "
        + "values(?,?,?,?)";

    private static final String DELETE_BLOB
        = "DELETE FROM " + BLOBS_TABLE + " where ledgerid=? and entryid=?";

    private static final String LIST_LEDGERS_BY_BUCKET
        = "SELECT bucketId,ledgerid FROM " + LEDGERS_TABLE + " where bucketId = ?";

    private static final String LIST_BLOBS_BY_LEDGER
        = "SELECT entryid,lastEntryId,size FROM " + BLOBS_TABLE + " where ledgerid = ?";

    private static final String LIST_DELETABLE_LEDGERS
        = "SELECT ledgerid "
        + " from " + LEDGERS_TABLE + " "
        + " WHERE ledgerid not in"
        + "(SELECT ledgerid from " + BLOBS_TABLE + " GROUP BY ledgerid)"
        + "";

    private static final String REGISTER_LEDGER
        = "INSERT "
        + "INTO " + LEDGERS_TABLE + "(bucketId, ledgerid, creationdate) "
        + "values(?,?,?)";

    private static final String DELETE_LEDGER
        = "DELETE FROM " + LEDGERS_TABLE + " where ledgerid=?";

    private Map<String, BucketMetadata> buckets;

    public HerdDBMetadataStorageManager(DataSource datasource,
        Configuration configuration) {
        this.bucketsTablespace = configuration.getBucketsTableSpace();
        this.datasource = datasource;
        this.bucketsTableSpacesReplicaCount = configuration.getReplicationFactor();
        this.useTablespaces = configuration.isUseTablespaces();
    }

    @Override
    public void createBucket(String name,
        String bucketTableSpaceName,
        BucketConfiguration configuration) throws ObjectManagerException {
        try (Connection connection = datasource.getConnection();
            PreparedStatement ps = connection.prepareStatement(SELECT_BUCKET);
            PreparedStatement psInsert = connection.prepareStatement(INSERT_BUCKET);) {

            if (useTablespaces) {
                ensureTablespace(bucketTableSpaceName, configuration.getReplicaCount());
                connection.setSchema(bucketsTablespace);
            }

            ps.setString(1, name);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return;
                }
            }

            psInsert.setString(1, name);
            psInsert.setString(2, bucketTableSpaceName);
            psInsert.setString(3, configuration.serialize());
            psInsert.executeUpdate();

            ensureTable(bucketTableSpaceName, LEDGERS_TABLE, CREATE_LEDGERS_TABLE);
            ensureTable(bucketTableSpaceName, BLOBS_TABLE, CREATE_BLOBS_TABLE);

            reloadBuckets();
        } catch (SQLException err) {
            throw new ObjectManagerException(err);
        }

    }

    private void ensureTablespace(String schema, int replicaCount) throws SQLException {
        if (!useTablespaces) {
            return;
        }
        try (Connection connection = datasource.getConnection();) {
            connection.setSchema(TableSpace.DEFAULT);
            DatabaseMetaData metaData = connection.getMetaData();
            boolean existTablespace;
            try (ResultSet schemas = metaData.getSchemas(null, schema);) {
                existTablespace = schemas.next();
            }
            if (!existTablespace) {
                try (Statement s = connection.createStatement();) {
                    s.executeUpdate(CREATE_TABLESPACE(schema, replicaCount));
                }
            }
        }

    }

    private void reloadBuckets() throws SQLException {
        try (Connection connection = datasource.getConnection();
            PreparedStatement load = connection.prepareStatement(LOAD_BUCKETS)) {
            if (useTablespaces) {
                connection.setSchema(bucketsTablespace);
            }
            Map<String, BucketMetadata> buckets = new HashMap<>();
            try (ResultSet rs = load.executeQuery()) {
                while (rs.next()) {
                    String id = rs.getString(1);
                    String tableSpace = rs.getString(2);
                    String configuration = rs.getString(3);

                    BucketMetadata bucket = new BucketMetadata(id,
                        BucketConfiguration.deserialize(configuration),
                        tableSpace);
                    buckets.put(id, bucket);
                }
            }
            this.buckets = buckets;
        }

    }

    private void ensureTable(String schema, String name, String createSql) throws SQLException {
        try (Connection connection = datasource.getConnection();) {
            if (useTablespaces) {
                connection.setSchema(schema);
            }
            DatabaseMetaData metaData = connection.getMetaData();
            boolean existTable;
            try (ResultSet rs = metaData.getTables(null, null, name, null)) {
                existTable = rs.next();
            }
            if (!existTable) {
                try (Statement s = connection.createStatement();) {
                    s.executeUpdate(createSql);
                }
            }
        }
    }

    @Override
    public void init() throws ObjectManagerException {
        try {
            ensureTablespace(bucketsTablespace, bucketsTableSpacesReplicaCount);
            ensureTable(bucketsTablespace, BUCKETS_TABLE, CREATE_BUCKETS_TABLE);
            reloadBuckets();
        } catch (SQLException err) {
            throw new ObjectManagerException(err);
        }

    }

    @Override
    public void registerObject(String bucketId,
        long ledgerId, long entryId, long lastEntryId, long size) throws ObjectManagerException {

        try (Connection connection = getConnectionForBucket(bucketId);
            PreparedStatement ps = connection.prepareStatement(REGISTER_BLOB)) {

            int i = 1;
            ps.setLong(i++, ledgerId);
            ps.setLong(i++, entryId);
            ps.setLong(i++, lastEntryId);
            ps.setLong(i++, size);
            ps.executeUpdate();
        } catch (SQLException err) {
            throw new ObjectManagerException(err);
        }
    }

    @Override
    public void deleteObject(String bucketId, long ledgerId, long entryId) throws ObjectManagerException {

        try (Connection connection = getConnectionForBucket(bucketId);
            PreparedStatement ps = connection.prepareStatement(DELETE_BLOB)) {

            int i = 1;
            ps.setLong(i++, ledgerId);
            ps.setLong(i++, entryId);
            ps.executeUpdate();
        } catch (SQLException err) {
            throw new ObjectManagerException(err);
        }
    }
    private static final Logger LOG = Logger.getLogger(HerdDBMetadataStorageManager.class.getName());

    @Override
    public Collection<BucketMetadata> listBuckets() throws ObjectManagerException {
        try {
            reloadBuckets();
            return new ArrayList<>(buckets.values());
        } catch (SQLException err) {
            throw new ObjectManagerException(err);
        }
    }

    @Override
    public Collection<Long> listDeletableLedgers(String bucketId) throws ObjectManagerException {

        try (Connection connection = getConnectionForBucket(bucketId);
            PreparedStatement ps = connection.prepareStatement(LIST_DELETABLE_LEDGERS);
            ResultSet rs = ps.executeQuery()) {
            List<Long> res = new ArrayList<>();
            while (rs.next()) {
                res.add(rs.getLong(1));
            }
            return res;
        } catch (SQLException err) {
            throw new ObjectManagerException(err);
        }
    }

    @Override
    public void registerLedger(String bucketId, long ledgerId) throws ObjectManagerException {

        try (Connection connection = getConnectionForBucket(bucketId);
            PreparedStatement ps = connection.prepareStatement(REGISTER_LEDGER);) {
            int i = 1;
            ps.setString(i++, bucketId);
            ps.setLong(i++, ledgerId);
            ps.setTimestamp(i++, new java.sql.Timestamp(System.currentTimeMillis()));
            ps.executeUpdate();
        } catch (SQLException err) {
            throw new ObjectManagerException(err);
        }
    }

    @Override
    public void deleteLedger(String bucketId, long ledgerId) throws ObjectManagerException {

        try (Connection connection = getConnectionForBucket(bucketId);
            PreparedStatement ps = connection.prepareStatement(DELETE_LEDGER);) {
            ps.setLong(1, ledgerId);
            ps.executeUpdate();
        } catch (SQLException err) {
            throw new ObjectManagerException(err);
        }
    }

    private Connection getConnectionForBucket(String bucketId) throws SQLException, ObjectManagerException {
        BucketMetadata bucket = getBucket(bucketId);
        Connection con = datasource.getConnection();
        if (useTablespaces) {
            con.setSchema(bucket.getTableSpaceName());
        }
        return con;
    }

    @Override
    public Collection<ObjectMetadata> listObjectsByLedger(String id, long ledgerId) throws ObjectManagerException {

        try (Connection connection = getConnectionForBucket(id);
            PreparedStatement ps = connection.prepareStatement(LIST_BLOBS_BY_LEDGER);) {

            ps.setLong(1, ledgerId);

            try (ResultSet rs = ps.executeQuery()) {
                List<ObjectMetadata> res = new ArrayList<>();

                while (rs.next()) {
                    BKEntryId entryId = new BKEntryId(ledgerId, rs.getLong(1), rs.getLong(2));
                    res.add(new ObjectMetadata(
                        entryId.toId(),
                        rs.getLong(3)
                    )
                    );
                }
                return res;
            }
        } catch (SQLException err) {
            throw new ObjectManagerException(err);
        }
    }

    @Override
    public Collection<LedgerMetadata> listLedgersbyBucketId(String id) throws ObjectManagerException {
        try (Connection connection = getConnectionForBucket(id);
            PreparedStatement ps = connection.prepareStatement(LIST_LEDGERS_BY_BUCKET);) {

            ps.setString(1, id);

            try (ResultSet rs = ps.executeQuery()) {
                List<LedgerMetadata> res = new ArrayList<>();
                while (rs.next()) {
                    res.add(new LedgerMetadata(rs.getString(1), rs.getLong(2)));
                }
                return res;
            }
        } catch (SQLException err) {
            throw new ObjectManagerException(err);
        }
    }

    @Override
    public void close() {
    }

    private BucketMetadata getBucket(String bucketId) throws ObjectManagerException {
        try {
            BucketMetadata bucket = buckets.get(bucketId);
            if (bucket != null) {
                return bucket;
            }
            reloadBuckets();
            bucket = buckets.get(bucketId);
            if (bucket == null) {
                throw new ObjectManagerException("No such bucket " + bucketId + ", only " + buckets.keySet());
            }
            return bucket;
        } catch (SQLException err) {
            throw new ObjectManagerException(err);
        }
    }

}
