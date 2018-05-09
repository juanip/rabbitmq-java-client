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


package com.rabbitmq.client.test.functional;

import static org.junit.Assert.assertNull;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.junit.Test;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.test.TestUtils;

public class Reject extends AbstractRejectTest
{
    @Test public void reject()
        throws IOException, InterruptedException
    {
        String q = channel.queueDeclare("", false, true, false, null).getQueue();

        InputStream m1  = TestUtils.getInputStreamMessage("1");
        InputStream m2  = TestUtils.getInputStreamMessage("2");

        basicPublishVolatile(m1, m1.available(), q);
        basicPublishVolatile(m2, m2.available(), q);

        long tag1 = checkDelivery(channel.basicGet(q, false), TestUtils.getInputStreamMessage("1"), false);
        long tag2 = checkDelivery(channel.basicGet(q, false), TestUtils.getInputStreamMessage("2"), false);
        QueueingConsumer c = new QueueingConsumer(secondaryChannel);
        String consumerTag = secondaryChannel.basicConsume(q, false, c);
        channel.basicReject(tag2, true);
        long tag3 = checkDelivery(c.nextDelivery(), TestUtils.getInputStreamMessage("2"), true);
        secondaryChannel.basicCancel(consumerTag);
        secondaryChannel.basicReject(tag3, false);
        assertNull(channel.basicGet(q, false));
        channel.basicAck(tag1, false);
        channel.basicReject(tag3, false);
        expectError(AMQP.PRECONDITION_FAILED);
    }
}
