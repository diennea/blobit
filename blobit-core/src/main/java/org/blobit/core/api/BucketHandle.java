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
import java.util.function.Consumer;

/**
 * Handles data inside a Bucket
 *
 * @author enrico.olivelli
 */
public interface BucketHandle {

    /**
     * Writes an object. This function is async, you have to check the result of
     * the Future in order to get the ID of the stored Object
     *
     * @param data
     * @return the value returned from the future will be an opaque 'printable'
     * id useful for retrival
     */
    public PutPromise put(byte[] data);

    /**
     * Writes an object. This function is async, you have to check the result of
     * the Future in order to get the ID of the stored Object This method does
     * not close the stream. In case of failure the status of the stream will be
     * undefined.
     *
     * @param length number of bytes to read from the stream
     * @param input
     * @return the value returned from the future will be an opaque 'printable'
     * id useful for retrival
     */
    public PutPromise put(long length, InputStream input);

    /**
     * Writes an object. This function is async, you have to check the result of
     * the Future in order to get the ID of the stored Object
     *
     * @param data array of bytes
     * @param offset offset from which to start writing bytes
     * @param len number of bytes to write
     * @return the value returned from the future will be an opaque 'printable'
     * id useful for retrival
     */
    public PutPromise put(byte[] data, int offset, int len);

    /**
     * Retrieves the contents of an object. This function is async, you have to
     * check the result of the Future in order to get the effective value. If a
     * null value is returned as byte[] it means that the object does not exits
     *
     * @param objectId
     * @return an handle to the operation
     */
    public GetPromise get(String objectId);

    /**
     * Retrieves the contents of an object.This function is async, you have to
     * check the result of the Future in order to get the effective value.The
     * returned handle will be completed when all data of the object have been
     * written to the OutputStream.In case of failure the status of the stream
     * will be undefined.This method does not close the stream.
     *
     * @param objectId
     * @param lengthCallback this callback will be called with the actual amount
     * of data which will be written to the stream
     * @param output destination of data
     * @param offset skip N bytes
     * @param length maximum amount of data to download, if -1 all the contents
     * of the object will be streamed
     * @return an handle to the operation
     */
    public DownloadPromise download(String objectId, Consumer<Long> lengthCallback, OutputStream output, int offset, long length);

    /**
     * Marks an object for deletion. Space will not be released immediately and
     * object would still be available to readers .
     *
     * @param objectId
     * @return an handle to the operation
     *
     * @see #gc()
     * @see #gc(java.lang.String)
     */
    public DeletePromise delete(String objectId);

}
