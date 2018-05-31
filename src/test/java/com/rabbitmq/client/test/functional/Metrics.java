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

import com.rabbitmq.client.*;
import com.rabbitmq.client.impl.StandardMetricsCollector;
import com.rabbitmq.client.impl.recovery.AutorecoveringConnection;
import com.rabbitmq.client.test.BrokerTestCase;
import com.rabbitmq.client.test.TestUtils;
import com.rabbitmq.tools.Host;
import org.awaitility.Duration;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

/**
 *
 */
public class Metrics extends BrokerTestCase {

    static final String QUEUE = "metrics.queue";

    @Override
    protected void createResources() throws IOException, TimeoutException {
        channel.queueDeclare(QUEUE, true, false, false, null);
    }

    @Override
    protected void releaseResources() throws IOException {
        channel.queueDelete(QUEUE);
    }

    @Test public void metricsStandardConnection() throws IOException, TimeoutException {
        doMetrics(createConnectionFactory());
    }

    @Test public void metricsAutoRecoveryConnection() throws IOException, TimeoutException {
        ConnectionFactory connectionFactory = createConnectionFactory();
        connectionFactory.setAutomaticRecoveryEnabled(true);
        doMetrics(connectionFactory);
    }

    private void doMetrics(ConnectionFactory connectionFactory) throws IOException, TimeoutException {
        StandardMetricsCollector metrics = new StandardMetricsCollector();
        connectionFactory.setMetricsCollector(metrics);
        Connection connection1 = null;
        Connection connection2 = null;
        try {
            connection1 = connectionFactory.newConnection();
            assertThat(metrics.getConnections().getCount(), is(1L));

            connection1.createChannel();
            connection1.createChannel();
            Channel channel = connection1.createChannel();
            assertThat(metrics.getChannels().getCount(), is(3L));

            sendMessage(channel);
            assertThat(metrics.getPublishedMessages().getCount(), is(1L));
            sendMessage(channel);
            assertThat(metrics.getPublishedMessages().getCount(), is(2L));

            channel.basicGet(QUEUE, true);
            assertThat(metrics.getConsumedMessages().getCount(), is(1L));
            channel.basicGet(QUEUE, true);
            assertThat(metrics.getConsumedMessages().getCount(), is(2L));
            channel.basicGet(QUEUE, true);
            assertThat(metrics.getConsumedMessages().getCount(), is(2L));

            connection2 = connectionFactory.newConnection();
            assertThat(metrics.getConnections().getCount(), is(2L));

            connection2.createChannel();
            channel = connection2.createChannel();
            assertThat(metrics.getChannels().getCount(), is(3L+2L));
            sendMessage(channel);
            sendMessage(channel);
            assertThat(metrics.getPublishedMessages().getCount(), is(2L+2L));

            channel.basicGet(QUEUE, true);
            assertThat(metrics.getConsumedMessages().getCount(), is(2L+1L));

            channel.basicConsume(QUEUE, true, new DefaultConsumer(channel));
            waitAtMost(timeout()).until(new ConsumedMessagesMetricsCallable(metrics), equalTo(2L+1L+1L));

            safeClose(connection1);
            waitAtMost(timeout()).until(new ConnectionsMetricsCallable(metrics), equalTo(1L));
            waitAtMost(timeout()).until(new ChannelsMetricsCallable(metrics), equalTo(2L));

            safeClose(connection2);
            waitAtMost(timeout()).until(new ConnectionsMetricsCallable(metrics), equalTo(0L));
            waitAtMost(timeout()).until(new ChannelsMetricsCallable(metrics), equalTo(0L));

            assertThat(metrics.getAcknowledgedMessages().getCount(), is(0L));
            assertThat(metrics.getRejectedMessages().getCount(), is(0L));

        } finally {
            safeClose(connection1);
            safeClose(connection2);
        }
    }

    @Test public void metricsAckStandardConnection() throws IOException, TimeoutException {
        doMetricsAck(createConnectionFactory());
    }

    @Test public void metricsAckAutoRecoveryConnection() throws IOException, TimeoutException {
        ConnectionFactory connectionFactory = createConnectionFactory();
        connectionFactory.setAutomaticRecoveryEnabled(true);
        doMetricsAck(connectionFactory);
    }

    private void doMetricsAck(ConnectionFactory connectionFactory) throws IOException, TimeoutException {
        StandardMetricsCollector metrics = new StandardMetricsCollector();
        connectionFactory.setMetricsCollector(metrics);

        Connection connection = null;
        try {
            connection = connectionFactory.newConnection();
            Channel channel1 = connection.createChannel();
            Channel channel2 = connection.createChannel();

            sendMessage(channel1);
            StreamGetResponse getResponse = channel1.basicGet(QUEUE, false);
            channel1.basicAck(getResponse.getEnvelope().getDeliveryTag(), false);
            assertThat(metrics.getConsumedMessages().getCount(), is(1L));
            assertThat(metrics.getAcknowledgedMessages().getCount(), is(1L));

            // basicGet / basicAck
            sendMessage(channel1);
            sendMessage(channel2);
            sendMessage(channel1);
            sendMessage(channel2);
            sendMessage(channel1);
            sendMessage(channel2);

            StreamGetResponse response1 = channel1.basicGet(QUEUE, false);
            StreamGetResponse response2 = channel2.basicGet(QUEUE, false);
            StreamGetResponse response3 = channel1.basicGet(QUEUE, false);
            StreamGetResponse response4 = channel2.basicGet(QUEUE, false);
            StreamGetResponse response5 = channel1.basicGet(QUEUE, false);
            StreamGetResponse response6 = channel2.basicGet(QUEUE, false);

            assertThat(metrics.getConsumedMessages().getCount(), is(1L+6L));
            assertThat(metrics.getAcknowledgedMessages().getCount(), is(1L));

            channel1.basicAck(response5.getEnvelope().getDeliveryTag(), false);
            assertThat(metrics.getAcknowledgedMessages().getCount(), is(1L+1L));
            channel1.basicAck(response3.getEnvelope().getDeliveryTag(), true);
            assertThat(metrics.getAcknowledgedMessages().getCount(), is(1L+1L+2L));

            channel2.basicAck(response2.getEnvelope().getDeliveryTag(), true);
            assertThat(metrics.getAcknowledgedMessages().getCount(), is(1L+(1L+2L)+1L));
            channel2.basicAck(response6.getEnvelope().getDeliveryTag(), true);
            assertThat(metrics.getAcknowledgedMessages().getCount(), is(1L+(1L+2L)+1L+2L));

            long alreadySentMessages = 1+(1+2)+1+2;

            // basicConsume / basicAck
            channel1.basicConsume(QUEUE, false, new MultipleAckConsumer(channel1, false));
            channel1.basicConsume(QUEUE, false, new MultipleAckConsumer(channel1, true));
            channel2.basicConsume(QUEUE, false, new MultipleAckConsumer(channel2, false));
            channel2.basicConsume(QUEUE, false, new MultipleAckConsumer(channel2, true));

            int nbMessages = 10;
            for(int i = 0; i < nbMessages; i++) {
                sendMessage(i%2 == 0 ? channel1 : channel2);
            }

            waitAtMost(timeout()).until(
                new ConsumedMessagesMetricsCallable(metrics),
                equalTo(alreadySentMessages+nbMessages)
            );

            waitAtMost(timeout()).until(
                new AcknowledgedMessagesMetricsCallable(metrics),
                equalTo(alreadySentMessages+nbMessages)
            );

        } finally {
            safeClose(connection);
        }
    }

    @Test public void metricsRejectStandardConnection() throws IOException, TimeoutException {
        doMetricsReject(createConnectionFactory());
    }

    @Test public void metricsRejectAutoRecoveryConnection() throws IOException, TimeoutException {
        ConnectionFactory connectionFactory = createConnectionFactory();
        connectionFactory.setAutomaticRecoveryEnabled(true);
        doMetricsReject(connectionFactory);
    }

    private void doMetricsReject(ConnectionFactory connectionFactory) throws IOException, TimeoutException {
        StandardMetricsCollector metrics = new StandardMetricsCollector();
        connectionFactory.setMetricsCollector(metrics);

        Connection connection = null;
        try {
            connection = connectionFactory.newConnection();
            Channel channel = connection.createChannel();

            sendMessage(channel);
            sendMessage(channel);
            sendMessage(channel);

            StreamGetResponse response1 = channel.basicGet(QUEUE, false);
            StreamGetResponse response2 = channel.basicGet(QUEUE, false);
            StreamGetResponse response3 = channel.basicGet(QUEUE, false);

            channel.basicReject(response2.getEnvelope().getDeliveryTag(), false);
            assertThat(metrics.getRejectedMessages().getCount(), is(1L));

            channel.basicNack(response3.getEnvelope().getDeliveryTag(), true, false);
            assertThat(metrics.getRejectedMessages().getCount(), is(1L+2L));
        } finally {
            safeClose(connection);
        }

    }

    @Test public void multiThreadedMetricsStandardConnection() throws InterruptedException, TimeoutException, IOException {
        doMultiThreadedMetrics(createConnectionFactory());
    }

    @Test public void multiThreadedMetricsAutoRecoveryConnection() throws InterruptedException, TimeoutException, IOException {
        ConnectionFactory connectionFactory = createConnectionFactory();
        connectionFactory.setAutomaticRecoveryEnabled(true);
        doMultiThreadedMetrics(connectionFactory);
    }

    private void doMultiThreadedMetrics(ConnectionFactory connectionFactory) throws IOException, TimeoutException, InterruptedException {
        StandardMetricsCollector metrics = new StandardMetricsCollector();
        connectionFactory.setMetricsCollector(metrics);
        int nbConnections = 3;
        int nbChannelsPerConnection = 5;
        int nbChannels = nbConnections * nbChannelsPerConnection;
        long nbOfMessages = 100;
        int nbTasks = nbChannels; // channel are not thread-safe

        Random random = new Random();

        // create connections
        Connection [] connections = new Connection[nbConnections];
        ExecutorService executorService = Executors.newFixedThreadPool(nbTasks);
        try {
            Channel [] channels = new Channel[nbChannels];
            for(int i = 0; i < nbConnections; i++) {
                connections[i] = connectionFactory.newConnection();
                for(int j = 0; j < nbChannelsPerConnection; j++) {
                    Channel channel = connections[i].createChannel();
                    channel.basicQos(1);
                    channels[i * nbChannelsPerConnection + j] = channel;
                }
            }

            // consume messages without ack
            for(int i = 0; i < nbOfMessages; i++) {
                sendMessage(channels[random.nextInt(nbChannels)]);
            }


            List<Callable<Void>> tasks = new ArrayList<Callable<Void>>();
            for(int i = 0; i < nbTasks; i++) {
                Channel channelForConsuming = channels[random.nextInt(nbChannels)];
                tasks.add(random.nextInt(10)%2 == 0 ?
                    new BasicGetTask(channelForConsuming, true) :
                    new BasicConsumeTask(channelForConsuming, true));
            }
            executorService.invokeAll(tasks);

            assertThat(metrics.getPublishedMessages().getCount(), is(nbOfMessages));
            waitAtMost(timeout()).until(new ConsumedMessagesMetricsCallable(metrics), equalTo(nbOfMessages));
            assertThat(metrics.getAcknowledgedMessages().getCount(), is(0L));

            // to remove the listeners
            for(int i = 0; i < nbChannels; i++) {
                channels[i].close();
                Channel channel = connections[random.nextInt(nbConnections)].createChannel();
                channel.basicQos(1);
                channels[i] = channel;
            }

            // consume messages with ack
            for(int i = 0; i < nbOfMessages; i++) {
                sendMessage(channels[random.nextInt(nbChannels)]);
            }

            executorService.shutdownNow();

            executorService = Executors.newFixedThreadPool(nbTasks);
            tasks = new ArrayList<Callable<Void>>();
            for(int i = 0; i < nbTasks; i++) {
                Channel channelForConsuming = channels[i];
                tasks.add(random.nextBoolean() ?
                    new BasicGetTask(channelForConsuming, false) :
                    new BasicConsumeTask(channelForConsuming, false));
            }
            executorService.invokeAll(tasks);

            assertThat(metrics.getPublishedMessages().getCount(), is(2*nbOfMessages));
            waitAtMost(timeout()).until(new ConsumedMessagesMetricsCallable(metrics), equalTo(2*nbOfMessages));
            waitAtMost(timeout()).until(new AcknowledgedMessagesMetricsCallable(metrics), equalTo(nbOfMessages));

            // to remove the listeners
            for(int i = 0; i < nbChannels; i++) {
                channels[i].close();
                Channel channel = connections[random.nextInt(nbConnections)].createChannel();
                channel.basicQos(1);
                channels[i] = channel;
            }

            // consume messages and reject them
            for(int i = 0; i < nbOfMessages; i++) {
                sendMessage(channels[random.nextInt(nbChannels)]);
            }

            executorService.shutdownNow();

            executorService = Executors.newFixedThreadPool(nbTasks);
            tasks = new ArrayList<Callable<Void>>();
            for(int i = 0; i < nbTasks; i++) {
                Channel channelForConsuming = channels[i];
                tasks.add(random.nextBoolean() ?
                    new BasicGetRejectTask(channelForConsuming) :
                    new BasicConsumeRejectTask(channelForConsuming));
            }
            executorService.invokeAll(tasks);

            assertThat(metrics.getPublishedMessages().getCount(), is(3*nbOfMessages));
            waitAtMost(timeout()).until(new ConsumedMessagesMetricsCallable(metrics), equalTo(3*nbOfMessages));
            waitAtMost(timeout()).until(new AcknowledgedMessagesMetricsCallable(metrics), equalTo(nbOfMessages));
            waitAtMost(timeout()).until(new RejectedMessagesMetricsCallable(metrics), equalTo(nbOfMessages));
        } finally {
            for (Connection connection : connections) {
                safeClose(connection);
            }
            executorService.shutdownNow();
        }

    }

    @Test public void errorInChannelStandardConnection() throws IOException, TimeoutException {
        errorInChannel(createConnectionFactory());
    }

    @Test public void errorInChananelAutoRecoveryConnection() throws IOException, TimeoutException {
        ConnectionFactory connectionFactory = createConnectionFactory();
        connectionFactory.setAutomaticRecoveryEnabled(true);
        errorInChannel(connectionFactory);
    }

    private void errorInChannel(ConnectionFactory connectionFactory) throws IOException, TimeoutException {
        StandardMetricsCollector metrics = new StandardMetricsCollector();
        connectionFactory.setMetricsCollector(metrics);

        Connection connection = null;
        try {
            connection = connectionFactory.newConnection();
            Channel channel = connection.createChannel();

            assertThat(metrics.getConnections().getCount(), is(1L));
            assertThat(metrics.getChannels().getCount(), is(1L));

            InputStream input = new ByteArrayInputStream("msg".getBytes("UTF-8"));
            channel.basicPublish("unlikelynameforanexchange", "", null, input, input.available());

            waitAtMost(timeout()).until(new ChannelsMetricsCallable(metrics), is(0L));
            assertThat(metrics.getConnections().getCount(), is(1L));
        } finally {
            safeClose(connection);
        }

    }

    @Test public void checkListenersWithAutoRecoveryConnection() throws Exception {
        ConnectionFactory connectionFactory = createConnectionFactory();
        connectionFactory.setNetworkRecoveryInterval(2000);
        connectionFactory.setAutomaticRecoveryEnabled(true);
        StandardMetricsCollector metrics = new StandardMetricsCollector();
        connectionFactory.setMetricsCollector(metrics);

        Connection connection = null;
        try {
            connection = connectionFactory.newConnection();

            Collection<?> shutdownHooks = getShutdownHooks(connection);
            assertThat(shutdownHooks.size(), is(0));

            connection.createChannel();

            assertThat(metrics.getConnections().getCount(), is(1L));
            assertThat(metrics.getChannels().getCount(), is(1L));

            closeAndWaitForRecovery((AutorecoveringConnection) connection);

            assertThat(metrics.getConnections().getCount(), is(1L));
            assertThat(metrics.getChannels().getCount(), is(1L));

            assertThat(shutdownHooks.size(), is(0));
        } finally {
            safeClose(connection);
        }

    }

    @Test public void checkAcksWithAutomaticRecovery() throws Exception {
        ConnectionFactory connectionFactory = createConnectionFactory();
        connectionFactory.setNetworkRecoveryInterval(2000);
        connectionFactory.setAutomaticRecoveryEnabled(true);
        StandardMetricsCollector metrics = new StandardMetricsCollector();
        connectionFactory.setMetricsCollector(metrics);

        Connection connection = null;
        try {
            connection = connectionFactory.newConnection();

            final Channel channel1 = connection.createChannel();
            final AtomicInteger ackedMessages = new AtomicInteger(0);

            channel1.basicConsume(QUEUE, false, new DefaultConsumer(channel1) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, InputStream body) throws IOException {
                    channel1.basicAck(envelope.getDeliveryTag(), false);
                    ackedMessages.incrementAndGet();
                }
            });

            Channel channel2 = connection.createChannel();
            channel2.confirmSelect();
            int nbMessages = 10;
            for (int i = 0; i < nbMessages; i++) {
                sendMessage(channel2);
            }
            channel2.waitForConfirms(1000);

            closeAndWaitForRecovery((AutorecoveringConnection) connection);

            for (int i = 0; i < nbMessages; i++) {
                sendMessage(channel2);
            }

            waitAtMost(timeout()).until(new Callable<Integer>() {
                @Override
                public Integer call() {
                    return ackedMessages.get();
                }
            }, equalTo(nbMessages * 2));

            assertThat(metrics.getConsumedMessages().getCount(), is((long) (nbMessages * 2)));
            assertThat(metrics.getAcknowledgedMessages().getCount(), is((long) (nbMessages * 2)));

        } finally {
            safeClose(connection);
        }
    }

    private ConnectionFactory createConnectionFactory() {
        ConnectionFactory connectionFactory = TestUtils.connectionFactory();
        return connectionFactory;
    }

    private void closeAndWaitForRecovery(AutorecoveringConnection connection) throws IOException, InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        connection.addRecoveryListener(new RecoveryListener() {
            public void handleRecovery(Recoverable recoverable) {
                latch.countDown();
            }

            @Override
            public void handleRecoveryStarted(Recoverable recoverable) {
                // no-op
            }
        });
        Host.closeConnection(connection);
        assertTrue(latch.await(5, TimeUnit.SECONDS));
    }

    private Collection<?> getShutdownHooks(Connection connection) throws NoSuchFieldException, IllegalAccessException {
        Field shutdownHooksField = connection.getClass().getDeclaredField("shutdownHooks");
        shutdownHooksField.setAccessible(true);
        return (Collection<?>) shutdownHooksField.get(connection);
    }

    private static class BasicGetTask implements Callable<Void> {

        final Channel channel;
        final boolean autoAck;
        final Random random = new Random();

        private BasicGetTask(Channel channel, boolean autoAck) {
            this.channel = channel;
            this.autoAck = autoAck;
        }

        @Override
        public Void call() throws Exception {
            StreamGetResponse getResponse = this.channel.basicGet(QUEUE, autoAck);
            if(!autoAck) {
                channel.basicAck(getResponse.getEnvelope().getDeliveryTag(), random.nextBoolean());
            }
            return null;
        }
    }

    private static class BasicConsumeTask implements Callable<Void> {

        final Channel channel;
        final boolean autoAck;
        final Random random = new Random();

        private BasicConsumeTask(Channel channel, boolean autoAck) {
            this.channel = channel;
            this.autoAck = autoAck;
        }

        @Override
        public Void call() throws Exception {
            this.channel.basicConsume(QUEUE, autoAck, new DefaultConsumer(channel) {

                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, InputStream body) throws IOException {
                    if(!autoAck) {
                        getChannel().basicAck(envelope.getDeliveryTag(), random.nextBoolean());
                    }
                }
            });
            return null;
        }
    }

    private static class BasicGetRejectTask implements Callable<Void> {

        final Channel channel;
        final Random random = new Random();

        private BasicGetRejectTask(Channel channel) {
            this.channel = channel;
        }

        @Override
        public Void call() throws Exception {
            StreamGetResponse response = channel.basicGet(QUEUE, false);
            if(response != null) {
                if(random.nextBoolean()) {
                    channel.basicNack(response.getEnvelope().getDeliveryTag(), random.nextBoolean(), false);
                } else {
                    channel.basicReject(response.getEnvelope().getDeliveryTag(), false);
                }
            }
            return null;
        }
    }

    private static class BasicConsumeRejectTask implements Callable<Void> {

        final Channel channel;
        final Random random = new Random();

        private BasicConsumeRejectTask(Channel channel) {
            this.channel = channel;
        }

        @Override
        public Void call() throws Exception {
            this.channel.basicConsume(QUEUE, false, new DefaultConsumer(channel) {

                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, InputStream body) throws IOException {
                    if(random.nextBoolean()) {
                        channel.basicNack(envelope.getDeliveryTag(), random.nextBoolean(), false);
                    } else {
                        channel.basicReject(envelope.getDeliveryTag(), false);
                    }
                }
            });
            return null;
        }
    }

    private void safeClose(Connection connection) {
        if(connection != null) {
            try {
                connection.abort();
            } catch (Exception e) {
                // OK
            }
        }
    }

    private void sendMessage(Channel channel) throws IOException {
        InputStream input = new ByteArrayInputStream("msg".getBytes("UTF-8"));
        channel.basicPublish("", QUEUE, null, input, input.available());
    }

    private Duration timeout() {
        return new Duration(10, TimeUnit.SECONDS);
    }

    private static class MultipleAckConsumer extends DefaultConsumer {

        final boolean multiple;

        public MultipleAckConsumer(Channel channel, boolean multiple) {
            super(channel);
            this.multiple = multiple;
        }

        @Override
        public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, InputStream body) throws IOException {
            try {
                Thread.sleep(new Random().nextInt(10));
            } catch (InterruptedException e) {
                throw new RuntimeException("Error during randomized wait",e);
            }
            getChannel().basicAck(envelope.getDeliveryTag(), multiple);
        }
    }

    static abstract class MetricsCallable implements Callable<Long> {

        final StandardMetricsCollector metrics;

        protected MetricsCallable(StandardMetricsCollector metrics) {
            this.metrics = metrics;
        }


    }

    static class ConnectionsMetricsCallable extends MetricsCallable {

        ConnectionsMetricsCallable(StandardMetricsCollector metrics) {
            super(metrics);
        }

        @Override
        public Long call() throws Exception {
            return metrics.getConnections().getCount();
        }
    }

    static class ChannelsMetricsCallable extends MetricsCallable {

        ChannelsMetricsCallable(StandardMetricsCollector metrics) {
            super(metrics);
        }

        @Override
        public Long call() throws Exception {
            return metrics.getChannels().getCount();
        }
    }

    static class PublishedMessagesMetricsCallable extends MetricsCallable {

        PublishedMessagesMetricsCallable(StandardMetricsCollector metrics) {
            super(metrics);
        }

        @Override
        public Long call() throws Exception {
            return metrics.getPublishedMessages().getCount();
        }
    }

    static class ConsumedMessagesMetricsCallable extends MetricsCallable {

        ConsumedMessagesMetricsCallable(StandardMetricsCollector metrics) {
            super(metrics);
        }

        @Override
        public Long call() throws Exception {
            return metrics.getConsumedMessages().getCount();
        }
    }

    static class AcknowledgedMessagesMetricsCallable extends MetricsCallable {

        AcknowledgedMessagesMetricsCallable(StandardMetricsCollector metrics) {
            super(metrics);
        }

        @Override
        public Long call() throws Exception {
            return metrics.getAcknowledgedMessages().getCount();
        }
    }

    static class RejectedMessagesMetricsCallable extends MetricsCallable {

        RejectedMessagesMetricsCallable(StandardMetricsCollector metrics) {
            super(metrics);
        }

        @Override
        public Long call() throws Exception {
            return metrics.getRejectedMessages().getCount();
        }
    }

}
