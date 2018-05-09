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

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.test.TestUtils;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Set;

import static com.rabbitmq.client.test.TestUtils.getInputStreamMessage;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class Nack extends AbstractRejectTest {

    @Test public void singleNack() throws Exception {
        String q =
            channel.queueDeclare("", false, true, false, null).getQueue();

        InputStream m1 = new ByteArrayInputStream("1".getBytes());
        InputStream m2 = new ByteArrayInputStream("2".getBytes());

        basicPublishVolatile(m1, m1.available(), q);
        basicPublishVolatile(m2, m2.available(), q);

        long tag1 = checkDelivery(channel.basicGet(q, false), getInputStreamMessage("1"), false);
        long tag2 = checkDelivery(channel.basicGet(q, false), getInputStreamMessage("2"), false);

        QueueingConsumer c = new QueueingConsumer(secondaryChannel);
        String consumerTag = secondaryChannel.basicConsume(q, false, c);

        // requeue
        channel.basicNack(tag2, false, true);

        long tag3 = checkDelivery(c.nextDelivery(), getInputStreamMessage("2"), true);
        secondaryChannel.basicCancel(consumerTag);

        // no requeue
        secondaryChannel.basicNack(tag3, false, false);

        assertNull(channel.basicGet(q, false));
        channel.basicAck(tag1, false);
        channel.basicNack(tag3, false, true);

        expectError(AMQP.PRECONDITION_FAILED);
    }

    @Test public void multiNack() throws Exception {
        String q =
            channel.queueDeclare("", false, true, false, null).getQueue();

        InputStream m1 = getInputStreamMessage("1");
        InputStream m2 = getInputStreamMessage("2");
        InputStream m3 = getInputStreamMessage("3");
        InputStream m4 = getInputStreamMessage("4");

        basicPublishVolatile(m1, m1.available(), q);
        basicPublishVolatile(m2, m2.available(), q);
        basicPublishVolatile(m3, m3.available(), q);
        basicPublishVolatile(m4, m4.available(), q);

        checkDelivery(channel.basicGet(q, false), getInputStreamMessage("1"), false);
        long tag1 = checkDelivery(channel.basicGet(q, false), getInputStreamMessage("2"), false);
        checkDelivery(channel.basicGet(q, false), getInputStreamMessage("3"), false);
        long tag2 = checkDelivery(channel.basicGet(q, false), getInputStreamMessage("4"), false);

        // ack, leaving a gap in un-acked sequence
        channel.basicAck(tag1, false);

        QueueingConsumer c = new QueueingConsumer(secondaryChannel);
        String consumerTag = secondaryChannel.basicConsume(q, false, c);

        // requeue multi
        channel.basicNack(tag2, true, true);

        long tag3 = checkDeliveries(c, getInputStreamMessage("1"), getInputStreamMessage("3"), getInputStreamMessage("4"));

        secondaryChannel.basicCancel(consumerTag);

        // no requeue
        secondaryChannel.basicNack(tag3, true, false);

        assertNull(channel.basicGet(q, false));

        channel.basicNack(tag3, true, true);

        expectError(AMQP.PRECONDITION_FAILED);
    }

    @Test public void nackAll() throws Exception {
        String q =
            channel.queueDeclare("", false, true, false, null).getQueue();

        InputStream m1 = getInputStreamMessage("1");
        InputStream m2 = getInputStreamMessage("2");

        basicPublishVolatile(m1, m1.available(), q);
        basicPublishVolatile(m2, m2.available(), q);

        checkDelivery(channel.basicGet(q, false), getInputStreamMessage("1"), false);
        checkDelivery(channel.basicGet(q, false), getInputStreamMessage("2"), false);

        // nack all
        channel.basicNack(0, true, true);

        QueueingConsumer c = new QueueingConsumer(secondaryChannel);
        String consumerTag = secondaryChannel.basicConsume(q, true, c);

        checkDeliveries(c, getInputStreamMessage("1"), getInputStreamMessage("2"));

        secondaryChannel.basicCancel(consumerTag);
    }

    private long checkDeliveries(QueueingConsumer c, InputStream... messages) throws InterruptedException, IOException {

        Set<String> msgSet = new HashSet<String>();
        for (InputStream message : messages) {
            msgSet.add(TestUtils.readString(message));
        }

        long lastTag = -1;
        for(int x = 0; x < messages.length; x++) {
            QueueingConsumer.Delivery delivery = c.nextDelivery();
            String m = TestUtils.readString(delivery.getBody());
            assertTrue("Unexpected message", msgSet.remove(m));
            checkDelivery(delivery, delivery.getBody(), true);
            lastTag = delivery.getEnvelope().getDeliveryTag();
        }

        assertTrue(msgSet.isEmpty());
        return lastTag;
    }
}
