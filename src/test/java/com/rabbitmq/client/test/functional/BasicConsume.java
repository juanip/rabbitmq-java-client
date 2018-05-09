package com.rabbitmq.client.test.functional;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.test.BrokerTestCase;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

/**
 *
 */
public class BasicConsume extends BrokerTestCase {

    @Test public void basicConsumeOk() throws IOException, InterruptedException {
        String q = channel.queueDeclare().getQueue();
        InputStream input1 = new ByteArrayInputStream("msg".getBytes("UTF-8"));
        basicPublishPersistent(input1, input1.available(), q);
        InputStream input2 = new ByteArrayInputStream("msg".getBytes("UTF-8"));
        basicPublishPersistent(input2, input2.available(), q);

        CountDownLatch latch = new CountDownLatch(2);
        channel.basicConsume(q, new CountDownLatchConsumer(channel, latch));

        boolean nbOfExpectedMessagesHasBeenConsumed = latch.await(1, TimeUnit.SECONDS);
        assertTrue("Not all the messages have been received", nbOfExpectedMessagesHasBeenConsumed);
    }

    static class CountDownLatchConsumer extends DefaultConsumer {

        private final CountDownLatch latch;

        public CountDownLatchConsumer(Channel channel, CountDownLatch latch) {
            super(channel);
            this.latch = latch;
        }

        @Override
        public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, InputStream body) throws IOException {
            latch.countDown();
        }
    }

}
