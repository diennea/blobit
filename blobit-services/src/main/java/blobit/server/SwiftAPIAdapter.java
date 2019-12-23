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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.io.InputStream;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.servlet.AsyncContext;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.commons.io.IOUtils;
import org.blobit.core.api.BucketConfiguration;
import org.blobit.core.api.BucketHandle;
import org.blobit.core.api.NamedObjectMetadata;
import org.blobit.core.api.ObjectManager;
import org.blobit.core.api.ObjectManagerException;
import org.blobit.core.api.ObjectNotFoundException;
import org.blobit.core.api.PutOptions;
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
                String name = remainingPath.substring(slash + 1);

                BucketHandle bucket = objectManager.getBucket(container);

                try {
                    NamedObjectMetadata byName = bucket.statByName(name);
                    LOG.log(Level.FINE, "[SWIFT] head object {0} -> {1}", new Object[]{name, byName});
                    if (byName == null) {
                        resp.sendError(HttpServletResponse.SC_NOT_FOUND, "Not found " + requestUri);
                        return;
                    }
                    resp.setContentLengthLong(byName.getSize());
                    resp.setStatus(HttpServletResponse.SC_OK);
                } catch (ObjectManagerException err) {
                    LOG.log(Level.SEVERE, "error", err);
                    resp.sendError(HttpServletResponse.SC_NOT_FOUND, "Internal error " + err);
                }
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
                String name = remainingPath.substring(slash + 1);

                LOG.log(Level.INFO, "[SWIFT] get {0}", new Object[]{name});
                BucketHandle bucket = objectManager.getBucket(container);
                AsyncContext startAsync = req.startAsync();
                bucket.downloadByName(name, (contentLength) -> {
                    LOG.log(Level.INFO, "[SWIFT] get object {0} -> len {1} bytes",
                            new Object[]{name, contentLength});
                    resp.setContentLengthLong(contentLength);
                    resp.setStatus(HttpServletResponse.SC_OK);
                    try {
                        // force switch to streaming mode
                        resp.flushBuffer();
                    } catch (IOException err) {
                    }
                }, startAsync.getResponse().getOutputStream(), 0 /* offset */, -1 /* maxlen */).future.
                        handle((v, error) -> {

                            try {
                                HttpServletResponse response = (HttpServletResponse) startAsync.getResponse();
                                if (error != null) {
                                    if (error instanceof ObjectNotFoundException) {
                                        response.sendError(HttpServletResponse.SC_NOT_FOUND);
                                    } else {
                                        LOG.log(Level.INFO, "[SWIFT] get object {0} finished, error {1}",
                                                new Object[]{name, error});
                                        response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                                    }
                                } else {
                                    LOG.log(Level.FINER, "[SWIFT] get object {0} finished",
                                            new Object[]{name});
                                    response.setStatus(HttpServletResponse.SC_OK);
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
                        FutureUtils.result(objectManager.createBucket(remainingPath, remainingPath,
                                BucketConfiguration.DEFAULT));
                        System.out.println("[SWIFT] create bucket " + remainingPath);
                        resp.setStatus(HttpServletResponse.SC_CREATED);
                    } else {
                        String container = remainingPath.substring(0, slash);
                        String name = remainingPath.substring(slash + 1);

                        BucketHandle bucket = objectManager.getBucket(container);
                        long expectedContentLen = req.getContentLength();
                        LOG.log(Level.INFO, "[SWIFT] put name={0} container={1} {3} bytes} ",
                                new Object[]{name, container, expectedContentLen});
                        if (expectedContentLen == -1L) {
                            // we must read the content, BlobIt needs to know the real size
                            try (InputStream in = req.getInputStream()) {
                                IOUtils.toByteArray(in);
                            }
                        } else {
                            AsyncContext startAsync = req.startAsync();
                            // streaming directly from client to bookkeeper
                            PutPromise prom = bucket.put(name, expectedContentLen, startAsync.getRequest().
                                    getInputStream(), PutOptions.DEFAULT_OPTIONS);
                            prom.future.handle((v, error) -> {

                                try {
                                    HttpServletResponse response = (HttpServletResponse) startAsync.getResponse();
                                    if (error != null) {
                                        LOG.log(Level.INFO, "[SWIFT] put object {0} finished, error {1}",
                                                new Object[]{name, error});
                                        response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, error + "");
                                    } else {
                                        LOG.log(Level.FINER, "[SWIFT] put object {0} finished",
                                                new Object[]{name});
                                        response.
                                                setStatus(HttpServletResponse.SC_CREATED);
                                    }
                                } catch (Exception err) {
                                    LOG.log(Level.SEVERE, "Error while putting object in streaming mode", err);
                                } finally {
                                    startAsync.complete();
                                }
                                return null;
                            });
                        }

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
                resp.sendError(HttpServletResponse.SC_METHOD_NOT_ALLOWED, "method " + req.getMethod()
                        + " not implemented");
                return;
        }
    }

}
