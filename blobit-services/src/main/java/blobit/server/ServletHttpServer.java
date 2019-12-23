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
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.bookkeeper.http.HttpServer;
import org.apache.bookkeeper.http.HttpServiceProvider;

/**
 * Simple Bookie HttpServer implementation as standard Servlet.
 * In order to use this HttpServer implementation you have to start an instance of {@link ServletHttpServerServlet}
 * and map it to '/'.
 */
@SuppressFBWarnings("ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD")
public class ServletHttpServer implements HttpServer {

    private static final Logger LOG = Logger.getLogger(ServletHttpServer.class.getName());

    private static HttpServiceProvider bookie;

    /**
     * Open the access to the Bookie to other components inside the same JVM.
     * @see ServletHttpServerImpl
     * @return the bookie or null.
     */
    public static HttpServiceProvider getBookie() {
        return bookie;
    }

    @Override
    public void initialize(HttpServiceProvider service) {
        bookie = service;
        LOG.log(Level.INFO, "Bookie HTTP Server inizialized: {0}", service);
    }

    @Override
    public boolean startServer(int i) {
        // NO-OP
        return true;
    }

    @Override
    public void stopServer() {
    }

    @Override
    public boolean isRunning() {
        return true;
    }

}
