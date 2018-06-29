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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils;
import org.blobit.core.api.BucketConfiguration;
import org.blobit.core.api.ObjectManager;
import org.blobit.core.api.ObjectManagerException;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import javax.servlet.AsyncContext;
import javax.servlet.annotation.WebServlet;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.blobit.core.api.BucketHandle;
import org.blobit.core.api.GetPromise;
import org.blobit.core.api.PutPromise;

/**
 * Emulates the OpenStack Swift Object API, only for using the CosBench
 *
 * @author enrico.olivelli
 */
@SuppressWarnings("serial")
@SuppressFBWarnings("SE_NO_SERIALVERSIONID")
@WebServlet(asyncSupported = true)
public class SwiftAPIAdapter extends HttpServlet {

    private static final Logger LOG = Logger.getLogger(SwiftAPIAdapter.class.getName());
    private static final String API_PATH = "/api/";

    @SuppressFBWarnings("SE_BAD_FIELD")
    private final ObjectManager objectManager;

    public SwiftAPIAdapter(ObjectManager objectManager) {
        this.objectManager = objectManager;
    }

    @Override
    @SuppressWarnings("empty-statement")
    protected void service(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        LOG.log(Level.INFO, "{0} {1}", new Object[]{req.getMethod(), req.getRequestURI()});

        String requestUri = req.getRequestURI();
        if (!requestUri.startsWith(API_PATH)) {
            resp.sendError(HttpServletResponse.SC_NOT_FOUND, "Not found " + requestUri);
            return;
        }
        switch (req.getMethod()) {
            case "HEAD": {
                String remainingPath = requestUri.substring(API_PATH.length());
                int slash = remainingPath.indexOf('/');
                if (slash <= 0) {
                    resp.sendError(HttpServletResponse.SC_NOT_FOUND, "Not found " + requestUri);
                    return;
                }
                String container = remainingPath.substring(0, slash);
                String objectId = remainingPath.substring(slash + 1);
                String name = remainingPath;

                LOG.log(Level.INFO, "[SWIFT] get object {0} as {1}", new Object[]{objectId, name});
                BucketHandle bucket = objectManager.getBucket(container);
                GetPromise byName = bucket.getByName(name);
                if (byName.id == null) {
                    resp.sendError(HttpServletResponse.SC_NOT_FOUND, "Not found " + requestUri);
                    return;
                }
                resp.setContentLengthLong(byName.length);
                resp.setStatus(HttpServletResponse.SC_OK);
                return;
            }
            case "GET": {
                String remainingPath = requestUri.substring(API_PATH.length());
                int slash = remainingPath.indexOf('/');
                if (slash <= 0) {
                    resp.sendError(HttpServletResponse.SC_NOT_FOUND, "Not found " + requestUri);
                    return;
                }
                String container = remainingPath.substring(0, slash);
                String objectId = remainingPath.substring(slash + 1);
                String name = remainingPath;

                LOG.log(Level.INFO, "[SWIFT] get object {0} as {1}", new Object[]{objectId, name});
                BucketHandle bucket = objectManager.getBucket(container);
                AsyncContext startAsync = req.startAsync();
                bucket.downloadByName(name, (contentLength) -> {
                    LOG.log(Level.INFO, "[SWIFT] get object {0} as {1} -> len {2} bytes", new Object[]{objectId, name, contentLength});
                    resp.setContentLengthLong(contentLength);
                    resp.setStatus(HttpServletResponse.SC_OK);
                    try {
                        resp.flushBuffer();
                    } catch (IOException err) {
                        err.printStackTrace();
                    }
                }, startAsync.getResponse().getOutputStream(), 0 /* offset */, -1 /* maxlen */).future
                        .handle((v, error) -> {
                            LOG.log(Level.INFO, "[SWIFT] get object {0} as {1} finished", new Object[]{objectId, name});
                            try {
                                if (error != null) {
                                    resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, error + "");
                                } else {
                                    resp.setStatus(HttpServletResponse.SC_OK);
                                }
                            } catch (Exception err) {
                                err.printStackTrace();
                            } finally {
                                startAsync.complete();
                            }
                            return null;
                        });
                return;
            }
            case "PUT": {
                String remainingPath = requestUri.substring(API_PATH.length());
                try {
                    int slash = remainingPath.indexOf('/');
                    if (slash <= 0) {
                        FutureUtils.result(objectManager.createBucket(remainingPath, remainingPath, BucketConfiguration.DEFAULT));
                        System.out.println("[SWIFT] create bucket " + remainingPath);
                        resp.setStatus(HttpServletResponse.SC_CREATED, "OK created bucket " + remainingPath);
                    } else {
                        String container = remainingPath.substring(0, slash);
                        String objectId = remainingPath.substring(slash + 1);
                        String name = remainingPath;
                        String resultId;
                        BucketHandle bucket = objectManager.getBucket(container);
                        long expectedContentLen = req.getContentLengthLong();
                        long realSize;
                        LOG.log(Level.INFO, "put {0} ((2} bytes) as {1} ", new Object[]{objectId, container, expectedContentLen});
//                        if (expectedContentLen == -1L) {
                            // we must read the content
                            try (InputStream in = req.getInputStream()) {
                                byte[] payload = IOUtils.toByteArray(in);
                                realSize = payload.length;
                                resultId = bucket.put(name, payload).get();
                            }
//                        } else {
//                            AsyncContext startAsync = req.startAsync();
//                            // streaming directly from client to bookkeeper
//                            PutPromise prom = bucket.put(name, expectedContentLen, startAsync.getRequest().getInputStream());
//                            prom.future.handle((v, error) -> {
//                                LOG.log(Level.INFO, "[SWIFT] get object {0} as {1} finished", new Object[]{objectId, name});
//                                try {
//                                    if (error != null) {
//                                        resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, error + "");
//                                    } else {
//                                        resp.setStatus(HttpServletResponse.SC_CREATED, "OK " + objectId + " as " + v);
//                                    }
//                                } catch (Exception err) {
//                                    err.printStackTrace();
//                                } finally {
//                                    startAsync.complete();
//                                }
//                                return null;
//                            });
//                            resultId = prom.id;
//                            realSize = expectedContentLen;
//                        }
                        LOG.log(Level.INFO, "put {0} ((3} vs {4} bytes) as {1} in {2}", new Object[]{objectId, resultId, container, expectedContentLen, realSize});

                    }
                } catch (InterruptedException err) {
                    resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, err + "");
                } catch (ObjectManagerException err) {
                    resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, err.getCause() + "");
                    LOG.log(Level.SEVERE, "Error while putting " + remainingPath, err.getCause());
                } catch (Exception err) {
                    resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, err.getCause() + "");
                    LOG.log(Level.SEVERE, "Error while putting " + remainingPath, err.getCause());
                }
                return;
            }
            default:
                resp.sendError(HttpServletResponse.SC_METHOD_NOT_ALLOWED, "method " + req.getMethod() + " not implemented");
                return;
        }
    }

}
