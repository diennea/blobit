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

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * This is the entry point of Blobit.
 *
 * @author enrico.olivelli
 */
public interface ObjectManager extends AutoCloseable {

    /**
     * Access to data inside of a Bucket. The Bucket must exist
     *
     * @param bucketId
     * @return The handle.
     *
     * @see #createBucket(java.lang.String, java.lang.String,
     * org.blobit.core.api.BucketConfiguration)
     * @see #getBucketMetadata(java.lang.String)
     */
    public BucketHandle getBucket(String bucketId);

    /**
     * Creates a new bucket to store data
     *
     * @param bucketId
     * @param tablespaceName
     * @param configuration
     */
    public CompletableFuture<BucketMetadata> createBucket(String bucketId, String tablespaceName, BucketConfiguration configuration);

    /**
     * Marks an existing bucket for deletion. Space will not be released
     * immediately.
     *
     * @param bucketId
     */
    public CompletableFuture<?> deleteBucket(String bucketId);

    /**
     * List every existing bucket.
     *
     * @param consumer
     * @throws ObjectManagerException
     */
    public void listBuckets(Consumer<BucketMetadata> consumer) throws ObjectManagerException;

    /**
     * Access metadata of a single bucket
     *
     * @return
     * @throws ObjectManagerException
     */
    public BucketMetadata getBucketMetadata(String bucketId) throws ObjectManagerException;

    /**
     * Loops over every bucket and performs {@link BucketHandle#gc()}. This
     * method can be called concurrencly by several clients in the cluster.
     * Usually this action iw performed periodically by server nodes.
     *
     * @see BucketHandle#gc()
     */
    public void gc();

    /**
     * Tries to cleaup and save resources due to deletion of buckets
     *
     * @throws ObjectManagerException
     */
    public void cleanup() throws ObjectManagerException;

    public void start() throws ObjectManagerException;

    @Override
    public void close();

}
