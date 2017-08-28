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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.blobit.core.api.BucketConfiguration;
import org.blobit.core.api.BucketMetadata;
import org.blobit.core.api.Configuration;
import org.blobit.core.api.LedgerMetadata;
import org.blobit.core.api.ObjectManagerException;
import org.blobit.core.api.ObjectMetadata;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import herddb.model.TableSpace;

/**
 * Stores metadata on HerdDB
 *
 * @author enrico.olivelli
 */
@SuppressFBWarnings("SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE")
public class HerdDBMetadataStorageManager {

    private static final String BUCKET_TABLE = "BUCKET";
    private static final String LEDGER_TABLE = "LEDGER";
    private static final String BLOB_TABLE = "BLOB";

    private String CREATE_TABLESPACE(String schema, int replicaCount) {
        return "CREATE TABLESPACE '" + schema + "','wait:60000','expectedreplicacount:" + replicaCount + "'";
    }

    /* ************** */
    /* *** BUCKET *** */
    /* ************** */

    private static final String CREATE_BUCKETS_TABLE =
            "CREATE TABLE " + BUCKET_TABLE + " (bucket_id STRING PRIMARY KEY, tablespace_name STRING, configuration STRING)";

    private static final String SELECT_BUCKET =
            "SELECT bucket_id,tablespace_name,configuration FROM " + BUCKET_TABLE + " WHERE bucket_id=?";

    private static final String LOAD_BUCKETS =
            "SELECT bucket_id,tablespace_name,configuration FROM " + BUCKET_TABLE;

    private static final String INSERT_BUCKET =
            "INSERT INTO " + BUCKET_TABLE + " (bucket_id,tablespace_name,configuration) VALUES (?,?,?)";

    /* ************** */
    /* *** LEDGER *** */
    /* ************** */

    private static final String CREATE_LEDGERS_TABLE =
            "CREATE TABLE " + LEDGER_TABLE + " (ledger_id LONG PRIMARY KEY, creation_date TIMESTAMP, bucket_id STRING)";

    private static final String REGISTER_LEDGER =
            "INSERT INTO " + LEDGER_TABLE + " (bucket_id,ledger_id,creation_date) VALUES (?,?,?)";

    private static final String DELETE_LEDGER =
            "DELETE FROM " + LEDGER_TABLE + " WHERE ledger_id=?";

    private static final String LIST_LEDGERS_BY_BUCKET =
            "SELECT bucket_id,ledger_id FROM " + LEDGER_TABLE + " WHERE bucket_id=?";

    private static final String LIST_DELETABLE_LEDGERS =
            "SELECT ledger_id FROM " + LEDGER_TABLE +
            " WHERE ledger_id NOT IN (SELECT ledger_id FROM " + BLOB_TABLE + " GROUP BY ledger_id)";

    /* ************** */
    /* **** BLOB **** */
    /* ************** */

    private static final String CREATE_BLOBS_TABLE =
            "CREATE TABLE " + BLOB_TABLE +
            " (ledger_id LONG, entry_id LONG, last_entry_id LONG, size LONG, PRIMARY KEY (ledger_id, entry_id))";

    private static final String REGISTER_BLOB =
            "INSERT INTO " + BLOB_TABLE + " (ledger_id, entry_id, last_entry_id, size) VALUES (?,?,?,?)";

    private static final String DELETE_BLOB
        = "DELETE FROM " + BLOB_TABLE + " WHERE ledger_id=? AND entry_id=?";

    private static final String LIST_BLOBS_BY_LEDGER
        = "SELECT entry_id,last_entry_id,size FROM " + BLOB_TABLE + " WHERE ledger_id=?";


    private final DataSource datasource;
    private final String bucketsTablespace;
    private final int bucketsTableSpacesReplicaCount;
    private final boolean useTablespaces;

    private Map<String, BucketMetadata> buckets;

    public HerdDBMetadataStorageManager(DataSource datasource,
        Configuration configuration) {
        this.bucketsTablespace = configuration.getBucketsTableSpace();
        this.datasource = datasource;
        this.bucketsTableSpacesReplicaCount = configuration.getReplicationFactor();
        this.useTablespaces = configuration.isUseTablespaces();
    }

    public void init() throws ObjectManagerException {
        try {
            ensureTablespace(bucketsTablespace, bucketsTableSpacesReplicaCount);
            ensureTable(bucketsTablespace, BUCKET_TABLE, CREATE_BUCKETS_TABLE);
            reloadBuckets();
        } catch (SQLException err) {
            throw new ObjectManagerException(err);
        }

    }

    public void createBucket(String bucketId,
        String tablespaceName,
        BucketConfiguration configuration) throws ObjectManagerException {
        try (Connection connection = datasource.getConnection();
            PreparedStatement ps = connection.prepareStatement(SELECT_BUCKET);
            PreparedStatement psInsert = connection.prepareStatement(INSERT_BUCKET);) {

            if (useTablespaces) {
                ensureTablespace(tablespaceName, configuration.getReplicaCount());
                connection.setSchema(bucketsTablespace);
            }

            ps.setString(1, bucketId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return;
                }
            }

            psInsert.setString(1, bucketId);
            psInsert.setString(2, tablespaceName);
            psInsert.setString(3, configuration.serialize());
            psInsert.executeUpdate();

            ensureTable(tablespaceName, LEDGER_TABLE, CREATE_LEDGERS_TABLE);
            ensureTable(tablespaceName, BLOB_TABLE, CREATE_BLOBS_TABLE);

            reloadBuckets();
        } catch (SQLException err) {
            throw new ObjectManagerException(err);
        }

    }

    public List<BucketMetadata> listBuckets() throws ObjectManagerException {
        try {
            reloadBuckets();
            return new ArrayList<>(buckets.values());
        } catch (SQLException err) {
            throw new ObjectManagerException(err);
        }
    }

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

    public void deleteLedger(String bucketId, long ledgerId) throws ObjectManagerException {

        try (Connection connection = getConnectionForBucket(bucketId);
            PreparedStatement ps = connection.prepareStatement(DELETE_LEDGER);) {
            ps.setLong(1, ledgerId);
            ps.executeUpdate();
        } catch (SQLException err) {
            throw new ObjectManagerException(err);
        }
    }

    public List<Long> listDeletableLedgers(String bucketId) throws ObjectManagerException {

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

    public List<LedgerMetadata> listLedgersbyBucketId(String id) throws ObjectManagerException {
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

    public List<ObjectMetadata> listObjectsByLedger(String id, long ledgerId) throws ObjectManagerException {

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

    /**
     * Returns a connection for given bucket.
     *
     * @param bucketId
     * @return
     * @throws SQLException
     * @throws ObjectManagerException if given bucket doesn't exists
     */
    private Connection getConnectionForBucket(String bucketId) throws SQLException, ObjectManagerException {
        return getConnectionForBucket(bucketId,true);
    }

    private Connection getConnectionForBucket(String bucketId, boolean autocommit) throws SQLException, ObjectManagerException {
        BucketMetadata bucket = getBucket(bucketId);
        Connection con = datasource.getConnection();
        if (useTablespaces) {
            con.setSchema(bucket.getTableSpaceName());
        }
        con.setAutoCommit(autocommit);
        return con;
    }

    private void ensureTablespace(String schema, int replicaCount) throws SQLException {
        if (!useTablespaces) {
            return;
        }
        try (Connection connection = datasource.getConnection()) {
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

    private void ensureTable(String schema, String name, String createSql) throws SQLException {
        try (Connection connection = datasource.getConnection()) {
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

}
