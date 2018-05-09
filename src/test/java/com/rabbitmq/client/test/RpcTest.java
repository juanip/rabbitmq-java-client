// Copyright (c) 2017 Pivotal Software, Inc.  All rights reserved.
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


package com.rabbitmq.client.test;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.RpcClient;
import com.rabbitmq.client.RpcServer;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class RpcTest {

    Connection clientConnection, serverConnection;
    Channel clientChannel, serverChannel;
    String queue = "rpc.queue";
    RpcServer rpcServer;


    @Before public void init() throws Exception {
        clientConnection = TestUtils.connectionFactory().newConnection();
        clientChannel = clientConnection.createChannel();
        serverConnection = TestUtils.connectionFactory().newConnection();
        serverChannel = serverConnection.createChannel();
        serverChannel.queueDeclare(queue, false, false, false, null);
    }

    @After public void tearDown() throws Exception {
        if(rpcServer != null) {
            rpcServer.terminateMainloop();
        }
        if(serverChannel != null) {
            serverChannel.queueDelete(queue);
        }
        clientConnection.close();
        serverConnection.close();
    }

    @Test
    public void rpc() throws Exception {
        rpcServer = new TestRpcServer(serverChannel, queue);
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    rpcServer.mainloop();
                } catch (Exception e) {
                    // safe to ignore when loops ends/server is canceled
                }
            }
        }).start();

        InputStream input = TestUtils.getInputStreamMessage("hello");
        RpcClient client = new RpcClient(clientChannel, "", queue, 1000);
        RpcClient.Response response = client.doCall(null, input, input.available());

        assertEquals("*** hello ***", TestUtils.readString(response.getBody()));
        assertEquals("pre-hello", response.getProperties().getHeaders().get("pre").toString());
        assertEquals("post-hello", response.getProperties().getHeaders().get("post").toString());
        client.close();
    }

    private static class TestRpcServer extends RpcServer {

        public TestRpcServer(Channel channel, String queueName) throws IOException {
            super(channel, queueName);
        }

        @Override
        protected AMQP.BasicProperties preprocessReplyProperties(QueueingConsumer.Delivery request, AMQP.BasicProperties.Builder builder) {
            Map<String, Object> headers = new HashMap<String, Object>();
            headers.put("pre", "pre-hello");
            builder.headers(headers);
            return builder.build();
        }

        @Override
        public InputStream handleCall(QueueingConsumer.Delivery request, AMQP.BasicProperties replyProperties) {
            InputStream body = request.getBody();
            String input = TestUtils.readStringQuietly(body);
            return TestUtils.getInputStreamMessage("*** " + input + " ***");
        }

        @Override
        protected AMQP.BasicProperties postprocessReplyProperties(QueueingConsumer.Delivery request, AMQP.BasicProperties.Builder builder) {
            Map<String, Object> headers = new HashMap<String, Object>(builder.build().getHeaders());
            headers.put("post", "post-hello");
            builder.headers(headers);
            return builder.build();
        }
    }
}
