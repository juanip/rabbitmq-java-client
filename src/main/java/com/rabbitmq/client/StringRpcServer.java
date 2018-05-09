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

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

/**
 * Subclass of RpcServer which accepts UTF-8 string requests.
 */
public class StringRpcServer extends RpcServer {
    public StringRpcServer(Channel channel) throws IOException
    { super(channel); }

    public StringRpcServer(Channel channel, String queueName) throws IOException
    { super(channel, queueName); }

    public static final String STRING_ENCODING = "UTF-8";

    /**
     * Overridden to do UTF-8 processing, and delegate to
     * handleStringCall. If UTF-8 is not understood by this JVM, falls
     * back to the platform default.
     */
    @Override
    @SuppressWarnings("unused")
    public InputStream handleCall(InputStream requestBody, AMQP.BasicProperties replyProperties)
    {
        return handleStringCall(requestBody, replyProperties);
    }

    /**
     * Delegates to handleStringCall(String).
     */
    public InputStream handleStringCall(InputStream request, AMQP.BasicProperties replyProperties)
    {
        return handleStringCall(request);
    }

    /**
     * Default implementation - override in subclasses. Returns the empty string.
     */
    public InputStream handleStringCall(InputStream request)
    {
        return new ByteArrayInputStream(new byte[0]);
    }

    /**
     * Overridden to do UTF-8 processing, and delegate to
     * handleStringCast. If requestBody cannot be interpreted as UTF-8
     * tries the platform default.
     */
    @Override
    public void handleCast(InputStream requestBody)
    {
        handleStringCast(requestBody);
    }

    /**
     * Default implementation - override in subclasses. Does nothing.
     */
    public void handleStringCast(InputStream requestBody) {
        // Do nothing.
    }
}
