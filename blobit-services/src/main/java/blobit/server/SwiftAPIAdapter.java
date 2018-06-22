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
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.blobit.core.api.BucketHandle;

/**
 * Emulates the OpenStack Swift Object API, only for using the CosBench
 *
 * @author enrico.olivelli
 */
@SuppressWarnings("serial")
@SuppressFBWarnings("SE_NO_SERIALVERSIONID")
public class SwiftAPIAdapter extends HttpServlet {

    private static final Logger LOG = Logger.getLogger(SwiftAPIAdapter.class.getName());
    private static final String API_PATH = "/api/";

    @SuppressFBWarnings("SE_BAD_FIELD")
    private final ObjectManager objectManager;

    public SwiftAPIAdapter(ObjectManager objectManager) {
        this.objectManager = objectManager;
    }

    @Override
    protected void service(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        LOG.log(Level.FINEST, "{0} {1}", new Object[]{req.getMethod(), req.getRequestURI()});

        String requestUri = req.getRequestURI();
        if (!requestUri.startsWith(API_PATH)) {
            resp.sendError(HttpServletResponse.SC_NOT_FOUND, "Not found " + requestUri);
            return;
        }
        switch (req.getMethod()) {
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

                LOG.log(Level.FINEST, "[SWIFT] get object {0} as {1}", new Object[]{objectId, name});
                BucketHandle bucket = objectManager.getBucket(container);
                bucket.downloadByName(name, (contentLength) -> {
                    resp.setContentLengthLong(contentLength);
                    resp.setStatus(HttpServletResponse.SC_OK);
                }, resp.getOutputStream(), 0 /* offset */, -1 /* maxlen */).future
                        .handle((v, error) -> {
                            try {
                                error.printStackTrace();
                                resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, error + "");
                            } catch (IOException err) {
                                err.printStackTrace();
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
//                        System.out.println("[SWIFT] create bucket " + remainingPath);
                        resp.setStatus(HttpServletResponse.SC_CREATED, "OK created bucket " + remainingPath);
                    } else {
                        String container = remainingPath.substring(0, slash);
                        String objectId = remainingPath.substring(slash + 1);
                        String name = remainingPath;
                        String resultId;
                        BucketHandle bucket = objectManager.getBucket(container);
                        long expectedContentLen = req.getContentLengthLong();
                        if (expectedContentLen == -1L) {
                            // we must read the content
                            try (InputStream in = req.getInputStream()) {
                                byte[] payload = IOUtils.toByteArray(in);
                                resultId = bucket.put(name, payload).get();
                            }
                        } else {
                            // streaming directly from client to bookkeeper
                            resultId = bucket.put(name, expectedContentLen, req.getInputStream()).get();
                        }
                        LOG.log(Level.FINEST, "put {0} ((3} bytes) as {1} in {2}", new Object[]{objectId, resultId, container, expectedContentLen});

                        resp.setStatus(HttpServletResponse.SC_CREATED, "OK " + objectId + " as " + resultId);
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
