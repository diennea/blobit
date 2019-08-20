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

import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Handles data inside a Bucket
 *
 * @author enrico.olivelli
 */
public interface BucketHandle {

    /**
     * Writes an object.This function is async, you have to check the result of the Future in order to get the ID of the
     * stored Object
     *
     * @param name the name of the object (optional)
     * @param data
     * @return the org.blobit.core.api.PutPromise
     */
    default PutPromise put(String name, byte[] data) {
        return put(name, data, 0, data.length, PutOptions.DEFAULT_OPTIONS);
    }

    /**
     * Writes an object.This function is async, you have to check the result of the Future in order to get the ID of the
     * stored Object This method does not close the stream. In case of failure the status of the stream will be
     * undefined.
     *
     * @param name the value of name  (optional)
     * @param length number of bytes to read from the stream
     * @param input the stream. it won't be closed automatically.
     * @param putOptions optoins
     * @return the org.blobit.core.api.PutPromise
     */
    PutPromise put(String name, long length, InputStream input, PutOptions putOptions);

    /**
     * Writes an object.This function is async, you have to check the result of the Future in order to get the ID of the
     * stored Object
     *
     * @param name the value of name  (optional)
     * @param data array of bytes
     * @param offset offset from which to start writing bytes
     * @param len number of bytes to write
     * @param putOptions optoins
     * @return the org.blobit.core.api.PutPromise
     */
    PutPromise put(String name, byte[] data, int offset, int len, PutOptions putOptions);

    /**
     * Atomically concatenates one named object to another one.
     * The source object will be implicitly deleted and
     * all of the blobs will now be part of the destination object,
     * appended to the tail.
     *
     * @param sourceName the name of an existing object, it will be deleted
     * @param destName the name of an existing object, or a new one
     *
     * @throws ObjectManagerException
     */
    void concat(String sourceName, String destName) throws ObjectManagerException;

    /**
     * Retrieves the contents of an object. This function is async, you have to check the result of the Future in order
     * to get the effective value. If a null value is returned as byte[] it means that the object does not exits
     *
     * @param objectId
     * @return an handle to the operation
     */
    GetPromise get(String objectId);

    /**
     * Retrieves the contents of an object
     *
     * @param name
     * @return an handle to the operation
     */
    NamedObjectGetPromise getByName(String name);

    /**
     * Retrieves the metadata of an object.
     *
     * @param name
     * @return the metadata, null if no object is found
     */
    NamedObjectMetadata statByName(String name) throws ObjectManagerException;

    /**
     * Retrieves the metadata of an object. Beware that metadata are stored on the object id itself, so this method may
     * return metadata even for object that have been deleted.
     *
     * @param objectId
     * @return the metadata
     */
    ObjectMetadata stat(String objectId) throws ObjectManagerException;

    /**
     * Retrieves detailed information about where data is stored for a particular object id.
     *
     * @param objectId
     * @return an handle to the result of the operation
     */
    CompletableFuture<? extends LocationInfo> getLocationInfo(String objectId) throws ObjectManagerException;

    /**
     * Retrieves the contents of an object.This function is async, you have to check the result of the Future in order
     * to get the effective value.The returned handle will be completed when all data of the object have been written to
     * the OutputStream.In case of failure the status of the stream will be undefined.This method does not close the
     * stream.
     *
     * @param objectId
     * @param lengthCallback this callback will be called with the actual amount of data which will be written to the
     * stream
     * @param output destination of data
     * @param offset skip N bytes
     * @param length maximum amount of data to download, if -1 all the contents of the object will be streamed
     * @return an handle to the operation
     */
    DownloadPromise download(String objectId, Consumer<Long> lengthCallback,
            OutputStream output, long offset, long length);

    /**
     * Retrieves the contents of an object.This function is async, you have to check the result of the Future in order
     * to get the effective value.The returned handle will be completed when all data of the object have been written to
     * the OutputStream.In case of failure the status of the stream will be undefined.This method does not close the
     * stream.
     *
     * @param name
     * @param lengthCallback this callback will be called with the actual amount of data which will be written to the
     * stream
     * @param output destination of data
     * @param offset skip N bytes
     * @param length maximum amount of data to download, if -1 all the contents of the object will be streamed
     * @return an handle to the operation
     */
    NamedObjectDownloadPromise downloadByName(String name,
            Consumer<Long> lengthCallback,
            OutputStream output, int offset,
            long length);

    /**
     * Marks an object for deletion. Space will not be released immediately and object would still be available to
     * readers .
     *
     * @param objectId
     * @return an handle to the operation
     *
     * @see #gc()
     */
    DeletePromise delete(String objectId);

    /**
     * Marks an object for deletion. Space will not be released immediately and object would still be available to
     * readers .
     *
     * @param name
     * @return an handle to the operation
     *
     * @see #gc()
     */
    NamedObjectDeletePromise deleteByName(String name);

    /**
     * Scans for a list of NamedObjects.Any action over metadata of the objects returned by this
     * method won't be executed in any kind of transaction.
     * The scan can be interrupted by returning 'false' from the NamedObjectConsumer callback.
     *
     * @param filter the filter
     * @param consumer the consumer.
     * @throws org.blobit.core.api.ObjectManagerException
     */
    void listByName(NamedObjectFilter filter, NamedObjectConsumer consumer) throws ObjectManagerException;

    /**
     * Release space allocated by a bucket but no more in use. This method can be called concurrently from several
     * clients in the cluster.
     *
     */
    void gc();

}
