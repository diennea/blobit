/*
 * Licensed to Diennea S.r.l. under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Diennea S.r.l. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package blobit.server;

import java.io.IOException;
import java.io.Writer;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.bookkeeper.http.AbstractHttpHandlerFactory;
import org.apache.bookkeeper.http.HttpRouter;
import org.apache.bookkeeper.http.HttpServer;
import org.apache.bookkeeper.http.HttpServiceProvider;
import org.apache.bookkeeper.http.service.ErrorHttpService;
import org.apache.bookkeeper.http.service.HttpEndpointService;
import org.apache.bookkeeper.http.service.HttpServiceRequest;
import org.apache.bookkeeper.http.service.HttpServiceResponse;
import org.apache.commons.io.IOUtils;

/**
 * Simple servlet that serves HTTP Bookie API.
 */
public class ServletHttpServerServlet extends HttpServlet {

    private static final Logger LOG = Logger.getLogger(ServletHttpServerServlet.class.getName());

    private final Map<String, HttpServer.ApiType> mappings = new ConcurrentHashMap<>();

    public ServletHttpServerServlet() {
        HttpRouter<HttpServer.ApiType> router = new HttpRouter<HttpServer.ApiType>(
                new AbstractHttpHandlerFactory<HttpServer.ApiType>(ServletHttpServer.getBookie()) {
            @Override
            public HttpServer.ApiType newHandler(HttpServer.ApiType at) {
                return at;
            }
        }) {
            @Override
            public void bindHandler(String endpoint, HttpServer.ApiType mapping) {
                mappings.put(endpoint, mapping);
            }
        };
        router.bindAll();
    }

    @Override
    protected void service(HttpServletRequest httpRequest, HttpServletResponse httpResponse) throws ServletException, IOException {
        HttpServiceRequest request = new HttpServiceRequest()
                .setMethod(convertMethod(httpRequest))
                .setParams(convertParams(httpRequest))
                .setBody(IOUtils.toString(httpRequest.getInputStream(), "UTF-8"));
        String uri = httpRequest.getRequestURI();
        HttpServiceResponse response;
        try {
            HttpServer.ApiType apiType = mappings.get(uri);
            HttpServiceProvider bookie = ServletHttpServer.getBookie();
            if (bookie == null) {
                httpResponse.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                return;
            }
            HttpEndpointService httpEndpointService = bookie.provideHttpEndpointService(apiType);
            if (httpEndpointService == null) {
                httpResponse.sendError(HttpServletResponse.SC_NOT_FOUND);
                return;
            }
            response = httpEndpointService.handle(request);
        } catch (Throwable e) {
            LOG.log(Level.SEVERE, "Error while service Bookie API request " + uri, e);
            response = new ErrorHttpService().handle(request);
        }
        if (response != null) {
            httpResponse.setStatus(response.getStatusCode());
            try (Writer out = httpResponse.getWriter()) {
                out.write(response.getBody());
            }
        } else {
            httpResponse.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        }
    }

    /**
     * Convert http request parameters to a map.
     */
    @SuppressWarnings("unchecked")
    Map<String, String> convertParams(HttpServletRequest request) {
        Map<String, String> map = new HashMap<>();
        for (Enumeration<String> param = request.getParameterNames();
                param.hasMoreElements();) {
            String pName = param.nextElement();
            map.put(pName, request.getParameter(pName));
        }
        return map;
    }

    /**
     * Convert http request method to the method that can be recognized by HttpServer.
     */
    HttpServer.Method convertMethod(HttpServletRequest request) {
        switch (request.getMethod()) {
            case "POST":
                return HttpServer.Method.POST;
            case "DELETE":
                return HttpServer.Method.DELETE;
            case "PUT":
                return HttpServer.Method.PUT;
            default:
                return HttpServer.Method.GET;
        }
    }
}
