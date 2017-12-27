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
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Stores metadata on HerdDB
 *
 * @author enrico.olivelli
 */
@SuppressFBWarnings("SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE")
public class HerdDBMetadataStorageManager {

    private static final String BUCKET_TABLE = "BUCKETS";
    private static final String BUCKET_TABLE_INDEX_BUCKET_ID = "BUCKET_BUCKETID";
    private static final String LEDGER_TABLE = "LEDGERS";
    private static final String BLOB_TABLE = "OBJECTS";

    private String CREATE_TABLESPACE(String schema, int replicaCount) {
        return "CREATE TABLESPACE '" + schema + "','wait:60000','expectedreplicacount:" + replicaCount + "'";
    }

    private String DROP_TABLESPACE(String schema) {
        return "DROP TABLESPACE '" + schema + "'";
    }


    /* ************** */
 /* *** BUCKET *** */
 /* ************** */
    private static final String CREATE_BUCKETS_TABLE
        = "CREATE TABLE " + BUCKET_TABLE + " ("
        + "    uuid STRING PRIMARY KEY,"
        + "    bucket_id STRING,"
        + "    status INTEGER,"
        + "    tablespace_name STRING,"
        + "    configuration STRING"
        + ")";

    private static final String CREATE_INDEX_ON_BUCKETS_TABLE
        = "CREATE INDEX " + BUCKET_TABLE_INDEX_BUCKET_ID + " ON " + BUCKET_TABLE + "(bucket_id)";

    private static final String SELECT_BUCKET
        = "SELECT bucket_id,uuid,status,tablespace_name,configuration FROM " + BUCKET_TABLE + " WHERE bucket_id=?";

    private static final String LOAD_BUCKETS_BY_STATUS
        = "SELECT bucket_id,uuid,status,tablespace_name,configuration FROM " + BUCKET_TABLE + " WHERE status=?";

    private static final String INSERT_BUCKET
        = "INSERT INTO " + BUCKET_TABLE + " (bucket_id,uuid,status,tablespace_name,configuration) VALUES (?,?,?,?,?)";

    private static final String MARK_BUCKET_FOR_DELETION
        = "UPDATE " + BUCKET_TABLE + " set status=" + BucketMetadata.STATUS_MARKED_FOR_DELETION + " WHERE bucket_id=?";

    private static final String DELETE_BUCKET_BY_UUID
        = "DELETE FROM " + BUCKET_TABLE + " WHERE uuid=? and bucket_id=?";


    /* ************** */
 /* *** LEDGER *** */
 /* ************** */
    private static final String CREATE_LEDGERS_TABLE
        = "CREATE TABLE " + LEDGER_TABLE + " ("
        + "    ledger_id LONG PRIMARY KEY,"
        + "    creation_date TIMESTAMP,"
        + "    bucket_uuid STRING"
        + ")";

    private static final String REGISTER_LEDGER
        = "INSERT INTO " + LEDGER_TABLE + " (bucket_uuid,ledger_id,creation_date) VALUES (?,?,?)";

    private static final String DELETE_LEDGER
        = "DELETE FROM " + LEDGER_TABLE + " WHERE ledger_id=?";

    private static final String DELETE_LEDGERS_BY_BUCKET_UUID
        = "DELETE FROM " + LEDGER_TABLE
        + " WHERE bucket_uuid =? ";

    private static final String LIST_LEDGERS_BY_BUCKET_UUID
        = "SELECT bucket_uuid,ledger_id FROM " + LEDGER_TABLE + " WHERE bucket_uuid=?";

    private static final String LIST_DELETABLE_LEDGERS
        = "SELECT ledger_id FROM " + LEDGER_TABLE
        + " WHERE NOT EXISTS (SELECT * FROM " + BLOB_TABLE + " b WHERE b.ledger_id=" + LEDGER_TABLE + ".ledger_id)";


    /* ************** */
 /* **** BLOB **** */
 /* ************** */
    private static final String CREATE_BLOBS_TABLE
        = "CREATE TABLE " + BLOB_TABLE
        + " (ledger_id LONG, entry_id LONG, last_entry_id LONG, size LONG, PRIMARY KEY (ledger_id, entry_id))";

    private static final String REGISTER_BLOB
        = "INSERT INTO " + BLOB_TABLE + " (ledger_id, entry_id, last_entry_id, size) VALUES (?,?,?,?)";

    private static final String DELETE_BLOB
        = "DELETE FROM " + BLOB_TABLE + " WHERE ledger_id=? AND entry_id=?";

    private static final String LIST_BLOBS_BY_LEDGER
        = "SELECT entry_id,last_entry_id,size FROM " + BLOB_TABLE + " WHERE ledger_id=?";

    private static final String DELETE_BLOBS_BY_BUCKET_UUID(String uuid) {
        return "DELETE FROM " + BLOB_TABLE
            + " WHERE ledger_id IN (SELECT ledger_id FROM " + LEDGER_TABLE + " WHERE bucket_uuid='" + uuid + "')";
    }

    private final DataSource datasource;
    private final String bucketsTablespace;
    private final int bucketsTableSpacesReplicaCount;
    private final boolean useTablespaces;
    private final boolean manageTablespaces;

    private Map<String, BucketMetadata> buckets;

    public HerdDBMetadataStorageManager(DataSource datasource,
        Configuration configuration) {
        this.bucketsTablespace = configuration.getBucketsTableSpace();
        this.datasource = datasource;
        this.bucketsTableSpacesReplicaCount = configuration.getReplicationFactor();
        this.useTablespaces = configuration.isUseTablespaces();
        this.manageTablespaces = configuration.isManageTablespaces();
    }

    public void init() throws ObjectManagerException {
        try {
            ensureTablespace(bucketsTablespace, bucketsTableSpacesReplicaCount);
            ensureTable(bucketsTablespace, BUCKET_TABLE, CREATE_BUCKETS_TABLE);
            ensureIndex(bucketsTablespace, BUCKET_TABLE, BUCKET_TABLE_INDEX_BUCKET_ID, CREATE_INDEX_ON_BUCKETS_TABLE);
            reloadBuckets();
        } catch (SQLException err) {
            throw new ObjectManagerException(err);
        }

    }

    public CompletableFuture<BucketMetadata> createBucket(String bucketId,
        String tablespaceName,
        BucketConfiguration configuration) {
        CompletableFuture res = new CompletableFuture();
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
                    res.complete(buildBucketMetadataFromResultSet(rs));
                    return res;
                }
            }

            String uuid = UUID.randomUUID().toString();
            psInsert.setString(1, bucketId);
            psInsert.setString(2, uuid);
            psInsert.setInt(3, BucketMetadata.STATUS_ACTIVE);
            psInsert.setString(4, tablespaceName);
            psInsert.setString(5, configuration.serialize());
            psInsert.executeUpdate();

            ensureTable(tablespaceName, LEDGER_TABLE, CREATE_LEDGERS_TABLE);
            ensureTable(tablespaceName, BLOB_TABLE, CREATE_BLOBS_TABLE);

            reloadBuckets();

            BucketMetadata result = new BucketMetadata(bucketId, uuid, BucketMetadata.STATUS_ACTIVE, configuration, tablespaceName);
            res.complete(result);
            return res;
        } catch (SQLException err) {
            res.completeExceptionally(err);
            return res;
        }

    }

    public void listBuckets(Consumer<BucketMetadata> consumer) throws ObjectManagerException {
        try {
            reloadBuckets();
            buckets.values().forEach(consumer);
        } catch (SQLException err) {
            throw new ObjectManagerException(err);
        }
    }

    public void registerLedger(String bucketId, long ledgerId) throws ObjectManagerException {

        try (Connection connection = getConnectionForBucket(bucketId);
            PreparedStatement ps = connection.prepareStatement(REGISTER_LEDGER);) {
            ps.setString(1, bucketId);
            ps.setLong(2, ledgerId);
            ps.setTimestamp(3, new java.sql.Timestamp(System.currentTimeMillis()));
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
            PreparedStatement ps = connection.prepareStatement(LIST_LEDGERS_BY_BUCKET_UUID);) {

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
            ps.setLong(1, ledgerId);
            ps.setLong(2, entryId);
            ps.setLong(3, lastEntryId);
            ps.setLong(4, size);
            ps.executeUpdate();
        } catch (SQLException err) {
            throw new ObjectManagerException(err);
        }
    }

    public void deleteObject(String bucketId, long ledgerId, long entryId) throws ObjectManagerException {

        try (Connection connection = getConnectionForBucket(bucketId);
            PreparedStatement ps = connection.prepareStatement(DELETE_BLOB)) {
            ps.setLong(1, ledgerId);
            ps.setLong(2, entryId);
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
        return getConnectionForBucket(bucketId, true);
    }

    private Connection getConnectionForBucket(String bucketId, boolean autocommit) throws SQLException, ObjectManagerException {
        BucketMetadata bucket = getBucket(bucketId);
        Connection con = getConnectionForBucketTableSpace(bucket);
        con.setAutoCommit(autocommit);
        return con;
    }

    private Connection getConnectionForBucketTableSpace(BucketMetadata bucket) throws SQLException {
        Connection con = datasource.getConnection();
        if (useTablespaces) {
            con.setSchema(bucket.getTableSpaceName());
        }
        return con;
    }

    private void ensureTablespace(String schema, int replicaCount) throws SQLException {
        if (!useTablespaces || !manageTablespaces) {
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

    private boolean existsTablespaceForBucket(String schema) throws SQLException {
        if (!useTablespaces) {
            return true;
        }
        try (Connection connection = datasource.getConnection()) {
            connection.setSchema(bucketsTablespace);
            DatabaseMetaData metaData = connection.getMetaData();
            try (ResultSet schemas = metaData.getSchemas(null, schema);) {
                return schemas.next();
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

    private void ensureIndex(String schema, String table, String name, String createSql) throws SQLException {
        try (Connection connection = datasource.getConnection()) {
            if (useTablespaces) {
                connection.setSchema(schema);
            }
            DatabaseMetaData metaData = connection.getMetaData();
            boolean existIndexInTable = false;
            try (ResultSet rs = metaData.getIndexInfo(null, null, table, false, false)) {
                while (rs.next()) {
                    String indexName = rs.getString(6);
                    if (indexName.equalsIgnoreCase(name)) {
                        existIndexInTable = true;
                        break;
                    }
                }

            }
            if (!existIndexInTable) {
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
            PreparedStatement load = connection.prepareStatement(LOAD_BUCKETS_BY_STATUS)) {
            load.setInt(1, BucketMetadata.STATUS_ACTIVE);
            if (useTablespaces) {
                connection.setSchema(bucketsTablespace);
            }
            Map<String, BucketMetadata> buckets = new HashMap<>();
            try (ResultSet rs = load.executeQuery()) {
                while (rs.next()) {
                    String id = rs.getString(1);
                    String uuid = rs.getString(2);
                    int status = rs.getInt(3);
                    String tableSpace = rs.getString(4);
                    String configuration = rs.getString(5);

                    BucketMetadata bucket = new BucketMetadata(id, uuid, status,
                        BucketConfiguration.deserialize(configuration),
                        tableSpace);
                    buckets.put(id, bucket);
                }
            }
            this.buckets = buckets;
        }

    }

    CompletableFuture<?> markBucketForDeletion(String bucketId) {
        CompletableFuture<?> res = new CompletableFuture<>();
        try (Connection connection = datasource.getConnection();
            PreparedStatement delete = connection.prepareStatement(MARK_BUCKET_FOR_DELETION)) {
            if (useTablespaces) {
                connection.setSchema(bucketsTablespace);
            }
            delete.setString(1, bucketId);
            int resDelete = delete.executeUpdate();
            if (resDelete <= 0) {
                res.completeExceptionally(new ObjectManagerException("bucket " + bucketId + " does not exist"));
                return res;
            }
            reloadBuckets();
            res.complete(null);
        } catch (SQLException err) {
            res.completeExceptionally(err);
        }
        return res;
    }

    BucketMetadata getBucketMetadata(String bucketId) throws ObjectManagerException {
        try {
            if (!buckets.containsKey(bucketId)) {
                reloadBuckets();
            }
            return buckets.get(bucketId);
        } catch (SQLException err) {
            throw new ObjectManagerException(err);
        }
    }

    List<BucketMetadata> selectBucketsMarkedForDeletion() throws ObjectManagerException {
        try (Connection connection = datasource.getConnection();
            PreparedStatement load = connection.prepareStatement(LOAD_BUCKETS_BY_STATUS)) {
            load.setInt(1, BucketMetadata.STATUS_MARKED_FOR_DELETION);
            if (useTablespaces) {
                connection.setSchema(bucketsTablespace);
            }
            List<BucketMetadata> buckets = new ArrayList<>();
            try (ResultSet rs = load.executeQuery()) {
                while (rs.next()) {
                    BucketMetadata bucket = buildBucketMetadataFromResultSet(rs);
                    buckets.add(bucket);
                }
            }
            return buckets;
        } catch (SQLException err) {
            throw new ObjectManagerException(err);
        }
    }

    private static BucketMetadata buildBucketMetadataFromResultSet(final ResultSet rs) throws SQLException {
        String id = rs.getString(1);
        String uuid = rs.getString(2);
        int status = rs.getInt(3);
        String tableSpace = rs.getString(4);
        String configuration = rs.getString(5);
        BucketMetadata bucket = new BucketMetadata(id, uuid, status,
            BucketConfiguration.deserialize(configuration),
            tableSpace);
        return bucket;
    }

    void cleanupDeletedBucketByUuid(BucketMetadata bucket) throws ObjectManagerException {
        try {
            if (!existsTablespaceForBucket(bucket.getTableSpaceName())) {
                LOG.log(Level.INFO, "Tablespace {0} already dropped for tablespace {1}", new Object[]{bucket.getTableSpaceName(), bucket.getBucketId()});
                return;
            }

            try (Connection connection = getConnectionForBucketTableSpace(bucket);
                PreparedStatement ps_delete_blobs = connection.prepareStatement(DELETE_BLOBS_BY_BUCKET_UUID(bucket.getUuid()));
                PreparedStatement ps_delete_ledgers = connection.prepareStatement(DELETE_LEDGERS_BY_BUCKET_UUID);) {
                ps_delete_ledgers.setString(1, bucket.getUuid());
                ps_delete_ledgers.executeUpdate();
                ps_delete_blobs.executeUpdate();
            }
        } catch (SQLException err) {
            throw new ObjectManagerException(err);
        }

    }
    private static final Logger LOG = Logger.getLogger(HerdDBMetadataStorageManager.class.getName());

    void deletedBucketByUuid(BucketMetadata bucket) throws ObjectManagerException {
        try (Connection connection = datasource.getConnection();
            PreparedStatement delete = connection.prepareStatement(DELETE_BUCKET_BY_UUID);) {
            if (useTablespaces) {
                connection.setSchema(bucketsTablespace);
            }
            dropBucketTableSpace(bucket.getTableSpaceName());

            delete.setString(1, bucket.getUuid());
            delete.setString(2, bucket.getBucketId());
            delete.executeUpdate();
        } catch (SQLException err) {
            throw new ObjectManagerException(err);
        }
    }

    private void dropBucketTableSpace(String tableSpaceName) throws SQLException {
        if (!useTablespaces || !manageTablespaces) {
            return;
        }
        try (Connection connection = datasource.getConnection()) {
            connection.setSchema(useTablespaces ? bucketsTablespace : TableSpace.DEFAULT);
            DatabaseMetaData metaData = connection.getMetaData();
            boolean existTablespace;
            try (ResultSet schemas = metaData.getSchemas(null, tableSpaceName);) {
                existTablespace = schemas.next();
            }
            if (!existTablespace) {
                return;
            }
            try (Statement s = connection.createStatement();) {
                s.executeUpdate(DROP_TABLESPACE(tableSpaceName));
            }
        }
    }

}
