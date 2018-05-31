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

package com.rabbitmq.client.test;

import com.rabbitmq.client.ConnectionFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

public class TestUtils {

    public static final boolean USE_NIO = System.getProperty("use.nio") == null ? false : true;

    public static ConnectionFactory connectionFactory() {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        if(USE_NIO) {
            connectionFactory.useNio();
        } else {
            connectionFactory.useBlockingIo();
        }
        return connectionFactory;
    }

    public static InputStream getInputStreamMessage(String message) {
       return new ByteArrayInputStream(message.getBytes());
    }

    public static String readString(InputStream input) throws IOException {
        byte[] bytes = new byte[input.available()];
        input.read(bytes);
        input.close();
        return new String(bytes);
    }

    public static String readStringQuietly(InputStream input) {
        try {
            return readString(input);
        } catch (IOException e) {
            return "";
        }
    }

}
