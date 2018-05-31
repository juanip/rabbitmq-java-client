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

package com.rabbitmq.client.test.server;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.test.BrokerTestCase;
import com.rabbitmq.tools.Host;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.fail;

public class TopicPermissions extends BrokerTestCase {

    String protectedTopic = "protected.topic";
    String notProtectedTopic = "not.protected.topic";
    String noneTopicExchange = "not.a.topic";

    @Override
    protected boolean shouldRun() throws IOException {
        return Host.isRabbitMqCtlCommandAvailable("set_topic_permissions");
    }

    @Override
    protected void createResources() throws IOException, TimeoutException {
        channel.exchangeDeclare(protectedTopic, BuiltinExchangeType.TOPIC);
        channel.exchangeDeclare(notProtectedTopic, BuiltinExchangeType.TOPIC);
        channel.exchangeDeclare(noneTopicExchange, BuiltinExchangeType.DIRECT);

        Host.rabbitmqctl("set_topic_permissions -p / guest " + protectedTopic + " \"^a\" \"^x\"");
        Host.rabbitmqctl("set_topic_permissions -p / guest " + noneTopicExchange + " \"^a\" \"^x\"");
    }

    @Override
    protected void releaseResources() throws IOException {
        channel.exchangeDelete(protectedTopic);
        channel.exchangeDelete(notProtectedTopic);
        channel.exchangeDelete(noneTopicExchange);

        Host.rabbitmqctl("clear_topic_permissions -p / guest");
    }

    @Test
    public void topicPermissions() throws IOException {
        assertAccessOk("Routing key matches on protected topic, should pass", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                InputStream input = new ByteArrayInputStream("content".getBytes());
                channel.basicPublish(protectedTopic, "a.b.c", null, input, input.available());
                channel.basicQos(0);
                return null;
            }
        });
        assertAccessRefused("Routing key does not match on protected topic, should not pass", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                InputStream input = new ByteArrayInputStream("content".getBytes());
                channel.basicPublish(protectedTopic, "b.c", null, input, input.available());
                channel.basicQos(0);
                return null;
            }
        });
        assertAccessOk("Message sent on not-protected exchange, should pass", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                InputStream input = new ByteArrayInputStream("content".getBytes());
                channel.basicPublish(notProtectedTopic, "a.b.c", null, input, input.available());
                channel.basicQos(0);
                return null;
            }
        });
        assertAccessOk("Routing key does not match on protected exchange, but not a topic, should pass", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                InputStream input = new ByteArrayInputStream("content".getBytes());
                channel.basicPublish(noneTopicExchange, "b.c", null, input, input.available());
                channel.basicQos(0);
                return null;
            }
        });
        assertAccessOk("Binding/unbinding on protected exchange with matching routing key, should pass", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                String queue = channel.queueDeclare().getQueue();
                channel.queueBind(queue, protectedTopic, "x.y.z");
                channel.basicQos(0);
                channel.queueUnbind(queue, protectedTopic, "x.y.z");
                channel.basicQos(0);
                return null;
            }
        });
        assertAccessRefused("Binding/unbinding on protected exchange with none-matching routing key, should not pass", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                String queue = channel.queueDeclare().getQueue();
                channel.queueBind(queue, protectedTopic, "y.z");
                channel.basicQos(0);
                channel.queueUnbind(queue, protectedTopic, "y.z");
                channel.basicQos(0);
                return null;
            }
        });
        assertAccessOk("Binding/unbinding on not-protected exchange with none-matching routing key, should pass", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                String queue = channel.queueDeclare().getQueue();
                channel.queueBind(queue, notProtectedTopic, "y.z");
                channel.basicQos(0);
                channel.queueUnbind(queue, notProtectedTopic, "y.z");
                channel.basicQos(0);
                return null;
            }
        });
    }

    void assertAccessOk(String description, Callable<Void> action) {
        try {
            action.call();
        } catch(Exception e) {
            fail(description + " (" + e.getMessage()+")");
        }
    }

    void assertAccessRefused(String description, Callable<Void> action) throws IOException {
        try {
            action.call();
            fail(description);
        } catch (IOException e) {
            checkShutdownSignal(AMQP.ACCESS_REFUSED, e);
            openChannel();
        } catch (AlreadyClosedException e) {
            checkShutdownSignal(AMQP.ACCESS_REFUSED, e);
            openChannel();
        } catch(Exception e) {
            fail("Unexpected exception: " + e.getMessage());
        }
    }
}
