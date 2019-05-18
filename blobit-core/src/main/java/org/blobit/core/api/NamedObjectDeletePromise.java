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

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.bookkeeper.common.concurrent.FutureUtils;

/**
 * Result of a Delete, it reports immediately the ID of the objects
 *
 * @author enrico.olivelli
 */
public final class NamedObjectDeletePromise {

    public final String name;
    public final List<String> id;
    public final CompletableFuture<?> future;

    public NamedObjectDeletePromise(String name, 
            List<String> id, CompletableFuture<?> future) {
        this.name = name;
        this.id = id;
        this.future = future;
    }

    public void get(long timeout, TimeUnit t) throws InterruptedException, ObjectManagerException, TimeoutException {
        try {
            FutureUtils.result(future, timeout, t);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw ie;
        } catch (ObjectManagerRuntimeException ie) {
            throw (ObjectManagerException) ie.getCause();
        } catch (TimeoutException | ObjectManagerException ie) {
            throw ie;
        } catch (Exception err) {
            throw new ObjectManagerException(err);
        }
    }

    public void get() throws InterruptedException, ObjectManagerException {
        try {
            FutureUtils.result(future);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw ie;
        } catch (ObjectManagerRuntimeException ie) {
            throw (ObjectManagerException) ie.getCause();
        } catch (ObjectManagerException ie) {
            throw ie;
        } catch (Exception err) {
            throw new ObjectManagerException(err);
        }
    }

}
