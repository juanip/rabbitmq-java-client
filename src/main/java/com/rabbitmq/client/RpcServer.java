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


package com.rabbitmq.client;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Class which manages a request queue for a simple RPC-style service.
 * The class is agnostic about the format of RPC arguments / return values.
*/
public class RpcServer {
    /** Channel we are communicating on */
    private final Channel _channel;
    /** Queue to receive requests from */
    private final String _queueName;
    /** Boolean controlling the exit from the mainloop. */
    private boolean _mainloopRunning = true;

    /** Consumer attached to our request queue */
    private QueueingConsumer _consumer;

    /**
     * Creates an RpcServer listening on a temporary exclusive
     * autodelete queue.
     */
    public RpcServer(Channel channel)
        throws IOException
    {
        this(channel, null);
    }

    /**
     * If the passed-in queue name is null, creates a server-named
     * temporary exclusive autodelete queue to use; otherwise expects
     * the queue to have already been declared.
     */
    public RpcServer(Channel channel, String queueName)
        throws IOException
    {
        _channel = channel;
        if (queueName == null || queueName.equals("")) {
            _queueName = _channel.queueDeclare().getQueue();
        } else {
            _queueName = queueName;
        }
        _consumer = setupConsumer();
    }

    /**
     * Public API - cancels the consumer, thus deleting the queue, if
     * it was a temporary queue, and marks the RpcServer as closed.
     * @throws IOException if an error is encountered
     */
    public void close()
        throws IOException
    {
        if (_consumer != null) {
            _channel.basicCancel(_consumer.getConsumerTag());
            _consumer = null;
        }
        terminateMainloop();
    }

    /**
     * Registers a consumer on the reply queue.
     * @throws IOException if an error is encountered
     * @return the newly created and registered consumer
     */
    protected QueueingConsumer setupConsumer()
        throws IOException
    {
        QueueingConsumer consumer = new QueueingConsumer(_channel);
        _channel.basicConsume(_queueName, consumer);
        return consumer;
    }

    /**
     * Public API - main server loop. Call this to begin processing
     * requests. Request processing will continue until the Channel
     * (or its underlying Connection) is shut down, or until
     * terminateMainloop() is called.
     *
     * Note that if the mainloop is blocked waiting for a request, the
     * termination flag is not checked until a request is received, so
     * a good time to call terminateMainloop() is during a request
     * handler.
     *
     * @return the exception that signalled the Channel shutdown, or null for orderly shutdown
     */
    public ShutdownSignalException mainloop()
        throws IOException
    {
        try {
            while (_mainloopRunning) {
                QueueingConsumer.Delivery request;
                try {
                    request = _consumer.nextDelivery();
                } catch (InterruptedException ie) {
                    continue;
                }
                processRequest(request);
                _channel.basicAck(request.getEnvelope().getDeliveryTag(), false);
            }
            return null;
        } catch (ShutdownSignalException sse) {
            return sse;
        }
    }

    /**
     * Call this method to terminate the mainloop.
     *
     * Note that if the mainloop is blocked waiting for a request, the
     * termination flag is not checked until a request is received, so
     * a good time to call terminateMainloop() is during a request
     * handler.
     */
    public void terminateMainloop() {
        _mainloopRunning = false;
    }

    /**
     * Private API - Process a single request. Called from mainloop().
     */
    public void processRequest(QueueingConsumer.Delivery request)
        throws IOException
    {
        AMQP.BasicProperties requestProperties = request.getProperties();
        String correlationId = requestProperties.getCorrelationId();
        String replyTo = requestProperties.getReplyTo();
        if (correlationId != null && replyTo != null)
        {
            AMQP.BasicProperties.Builder replyPropertiesBuilder
                = new AMQP.BasicProperties.Builder().correlationId(correlationId);
            AMQP.BasicProperties replyProperties = preprocessReplyProperties(request, replyPropertiesBuilder);
            InputStream replyBody = handleCall(request, replyProperties);
            replyProperties = postprocessReplyProperties(request, replyProperties.builder());
            _channel.basicPublish("", replyTo, replyProperties, replyBody, replyBody.available());
        } else {
            handleCast(request);
        }
    }

    /**
     * Lowest-level response method. Calls
     * handleCall(AMQP.BasicProperties,byte[],AMQP.BasicProperties).
     */
    public InputStream handleCall(QueueingConsumer.Delivery request,
                             AMQP.BasicProperties replyProperties)
    {
        return handleCall(request.getProperties(),
                          request.getBody(),
                          replyProperties);
    }

    /**
     * Mid-level response method. Calls
     * handleCall(byte[],AMQP.BasicProperties).
     */
    public InputStream handleCall(AMQP.BasicProperties requestProperties,
                             InputStream requestBody,
                             AMQP.BasicProperties replyProperties)
    {
        return handleCall(requestBody, replyProperties);
    }

    /**
     * High-level response method. Returns an empty response by
     * default - override this (or other handleCall and handleCast
     * methods) in subclasses.
     */
    public InputStream handleCall(InputStream requestBody,
                             AMQP.BasicProperties replyProperties)
    {
        return new ByteArrayInputStream(new byte[0]);
    }

    /**
     * Gives a chance to set/modify reply properties before handling call.
     * Note the correlationId property is already set.
     * @param request the inbound message
     * @param builder the reply properties builder
     * @return the properties to pass in to the handling call
     */
    protected AMQP.BasicProperties preprocessReplyProperties(QueueingConsumer.Delivery request, AMQP.BasicProperties.Builder builder) {
        return builder.build();
    }

    /**
     * Gives a chance to set/modify reply properties after the handling call
     * @param request the inbound message
     * @param builder the reply properties builder
     * @return the properties to pass in to the response message
     */
    protected AMQP.BasicProperties postprocessReplyProperties(QueueingConsumer.Delivery request, AMQP.BasicProperties.Builder builder) {
        return builder.build();
    }

    /**
     * Lowest-level handler method. Calls
     * handleCast(AMQP.BasicProperties,byte[]).
     */
    public void handleCast(QueueingConsumer.Delivery request)
    {
        handleCast(request.getProperties(), request.getBody());
    }

    /**
     * Mid-level handler method. Calls
     * handleCast(byte[]).
     */
    public void handleCast(AMQP.BasicProperties requestProperties, InputStream requestBody)
    {
        handleCast(requestBody);
    }

    /**
     * High-level handler method. Does nothing by default - override
     * this (or other handleCast and handleCast methods) in
     * subclasses.
     */
    public void handleCast(InputStream requestBody)
    {
        // Does nothing.
    }

    /**
     * Retrieve the channel.
     * @return the channel to which this server is connected
     */
    public Channel getChannel() {
        return _channel;
    }

    /**
     * Retrieve the queue name.
     * @return the queue which this server is consuming from
     */
    public String getQueueName() {
        return _queueName;
    }
}

