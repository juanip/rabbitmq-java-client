// Copyright (c) 2007-Present Pivotal Software, Inc.  All rights reserved.
//
// This software, the RabbitMQ Java client library, is triple-licensed under the
// Mozilla Public License 1.1 ("MPL"), the GNU General Public License version 2
// ("GPL") and the Apache License version 2 ("ASL"). For the MPL, please see
// LICENSE-MPL-RabbitMQ. For the GPL, please see LICENSE-GPL2.  For the ASL,
// please see LICENSE-APACHE2.
//
// This software is distributed on an "AS IS" basis, WITHOUT WARRANTY OF ANY KIND,
// either express or implied. See the LICENSE file for specific language governing
// rights and limitations of this software.
//
// If you have any questions regarding licensing, please contact us at
// info@rabbitmq.com.


package com.rabbitmq.tools.jsonrpc;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.StringRpcServer;
import com.rabbitmq.client.impl.VolatileFileOutputStream;
import com.rabbitmq.tools.json.JSONReader;
import com.rabbitmq.tools.json.JSONWriter;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * JSON-RPC Server class.
 *
 * Given a Java {@link Class}, representing an interface, and an
 * implementation of that interface, JsonRpcServer will reflect on the
 * class to construct the {@link ServiceDescription}, and will route
 * incoming requests for methods on the interface to the
 * implementation object while the mainloop() is running.
 *
 * @see com.rabbitmq.client.RpcServer
 * @see JsonRpcClient
 */
public class JsonRpcServer extends StringRpcServer {
    /**
     * Holds the JSON-RPC service description for this client.
     */
    public ServiceDescription serviceDescription;
    /**
     * The interface this server implements.
     */
    public Class<?> interfaceClass;
    /**
     * The instance backing this server.
     */
    public Object interfaceInstance;

    /**
     * Construct a server that talks to the outside world using the
     * given channel, and constructs a fresh temporary
     * queue. Use getQueueName() to discover the created queue name.
     *
     * @param channel
     *         AMQP channel to use
     * @param interfaceClass
     *         Java interface that this server is exposing to the world
     * @param interfaceInstance
     *         Java instance (of interfaceClass) that is being exposed
     * @throws IOException
     *         if something goes wrong during an AMQP operation
     */
    public JsonRpcServer(Channel channel, Class<?> interfaceClass, Object interfaceInstance) throws IOException {
        super(channel);
        init(interfaceClass, interfaceInstance);
    }

    private void init(Class<?> interfaceClass, Object interfaceInstance) {
        this.interfaceClass = interfaceClass;
        this.interfaceInstance = interfaceInstance;
        this.serviceDescription = new ServiceDescription(interfaceClass);
    }

    /**
     * Construct a server that talks to the outside world using the
     * given channel and queue name. Our superclass,
     * RpcServer, expects the queue to exist at the time of
     * construction.
     *
     * @param channel
     *         AMQP channel to use
     * @param queueName
     *         AMQP queue name to listen for requests on
     * @param interfaceClass
     *         Java interface that this server is exposing to the world
     * @param interfaceInstance
     *         Java instance (of interfaceClass) that is being exposed
     * @throws IOException
     *         if something goes wrong during an AMQP operation
     */
    public JsonRpcServer(Channel channel, String queueName, Class<?> interfaceClass, Object interfaceInstance)
            throws IOException {
        super(channel, queueName);
        init(interfaceClass, interfaceInstance);
    }

    /**
     * Override our superclass' method, dispatching to doCall.
     */
    @Override
    public InputStream handleStringCall(InputStream requestBody, AMQP.BasicProperties replyProperties) {
        try {
            return doCall(convertToString(requestBody));
        }
        catch (IOException e) {
            throw new RuntimeException("Sorry!");
        }
    }

    private static String convertToString(final InputStream is) throws IOException {
        final StringBuilder out = new StringBuilder();
        Reader reader = null;
        try {
            reader = new BufferedReader(new InputStreamReader(is, STRING_ENCODING));
            int c;
            while ((c = reader.read()) != -1) {
                out.append((char) c);
            }
            return out.toString();
        }
        finally {
            if (reader != null) {
                reader.close();
            }
        }
    }

    /**
     * Runs a single JSON-RPC request.
     *
     * @param requestBody
     *         the JSON-RPC request string (a JSON encoded value)
     * @return a JSON-RPC response string (a JSON encoded value)
     */
    private InputStream doCall(String requestBody) throws IOException {
        Object id;
        String method;
        Object[] params;
        try {
            @SuppressWarnings("unchecked") Map<String, Object> request = (Map<String, Object>) new JSONReader().read(
                    requestBody);
            if (request == null) {
                return errorResponse(null, 400, "Bad Request", null);
            }
            if (!ServiceDescription.JSON_RPC_VERSION.equals(request.get("version"))) {
                return errorResponse(null, 505, "JSONRPC version not supported", null);
            }

            id = request.get("id");
            method = (String) request.get("method");
            List<?> parmList = (List<?>) request.get("params");
            params = parmList.toArray();
        }
        catch (ClassCastException cce) {
            // Bogus request!
            return errorResponse(null, 400, "Bad Request", null);
        }

        if (method.equals("system.describe")) {
            return resultResponse(id, serviceDescription);
        }
        else if (method.startsWith("system.")) {
            return errorResponse(id, 403, "System methods forbidden", null);
        }
        else {
            Object result;
            try {
                result = matchingMethod(method, params).invoke(interfaceInstance, params);
            }
            catch (Throwable t) {
                return errorResponse(id, 500, "Internal Server Error", t);
            }
            return resultResponse(id, result);
        }
    }

    /**
     * Retrieves the best matching method for the given method name and parameters.
     *
     * Subclasses may override this if they have specialised
     * dispatching requirements, so long as they continue to honour
     * their ServiceDescription.
     */
    public Method matchingMethod(String methodName, Object[] params) {
        ProcedureDescription proc = serviceDescription.getProcedure(methodName, params.length);
        return proc.internal_getMethod();
    }

    /**
     * Construct and encode a JSON-RPC error response for the request
     * ID given, using the code, message, and possible
     * (JSON-encodable) argument passed in.
     */
    public static InputStream errorResponse(Object id, int code, String message, Object errorArg) throws IOException {
        Map<String, Object> err = new HashMap<String, Object>();
        err.put("name", "JSONRPCError");
        err.put("code", code);
        err.put("message", message);
        err.put("error", errorArg);
        return response(id, "error", err);
    }

    /**
     * Construct and encode a JSON-RPC success response for the
     * request ID given, using the result value passed in.
     */
    public static InputStream resultResponse(Object id, Object result) throws IOException {
        return response(id, "result", result);
    }

    /**
     * Private API - used by errorResponse and resultResponse.
     */
    public static InputStream response(Object id, String label, Object value) throws IOException {
        Map<String, Object> resp = new HashMap<String, Object>();
        resp.put("version", ServiceDescription.JSON_RPC_VERSION);
        if (id != null) {
            resp.put("id", id);
        }
        resp.put(label, value);
        return read(new JSONWriter().write(resp));
    }

    private static InputStream read(String str) throws IOException {
        VolatileFileOutputStream bos = null;
        try {
            bos = new VolatileFileOutputStream();
            bos.write(str.getBytes());
            return bos.toInputStream();
        }
        finally {
            if (bos != null) {
                bos.close();
            }
        }
    }

    /**
     * Public API - gets the service description record that this
     * service built from interfaceClass at construction time.
     */
    public ServiceDescription getServiceDescription() {
        return serviceDescription;
    }
}
