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
import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.ReturnListener;
import com.rabbitmq.client.StreamGetResponse;
import com.rabbitmq.client.test.BrokerTestCase;
import com.rabbitmq.utility.BlockingCell;

import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class Routing extends BrokerTestCase
{

    protected final String E = "MRDQ";
    protected final String Q1 = "foo";
    protected final String Q2 = "bar";

    private volatile BlockingCell<Integer> returnCell;

    protected void createResources() throws IOException {
        channel.exchangeDeclare(E, "direct");
        channel.queueDeclare(Q1, false, false, false, null);
        channel.queueDeclare(Q2, false, false, false, null);
    }

    protected void releaseResources() throws IOException {
        channel.queueDelete(Q1);
        channel.queueDelete(Q2);
        channel.exchangeDelete(E);
    }

    private void bind(String queue, String routingKey)
        throws IOException
    {
        channel.queueBind(queue, E, routingKey);
    }

    private void check(String routingKey, boolean expectQ1, boolean expectQ2)
        throws IOException
    {
        InputStream input = new ByteArrayInputStream("mrdq".getBytes());
        channel.basicPublish(E, routingKey, null, input, input.available());
        checkGet(Q1, expectQ1);
        checkGet(Q2, expectQ2);
    }

    private void checkGet(String queue, boolean messageExpected)
        throws IOException
    {
        StreamGetResponse r = channel.basicGet(queue, true);
        if (messageExpected) {
            assertNotNull(r);
        } else {
            assertNull(r);
        }
    }

    /**
     * Tests the "default queue name" and "default routing key" pieces
     * of the spec. See the doc for the "queue" and "routing key"
     * fields of queue.bind.
     */
    @Test public void mRDQRouting()
        throws IOException
    {
        bind(Q1, "baz");        //Q1, "baz"
        bind(Q1, "");           //Q1, ""
        bind("", "baz");        //Q2, "baz"
        bind("", "");           //Q2, Q2
        check("", true, false);
        check(Q1, false, false);
        check(Q2, false, true);
        check("baz", true, true);
    }

    /**
     * If a queue has more than one binding to an exchange, it should
     * NOT receive duplicate copies of a message that matches both
     * bindings.
     */
    @Test public void doubleBinding()
        throws IOException
    {
        channel.queueBind(Q1, "amq.topic", "x.#");
        channel.queueBind(Q1, "amq.topic", "#.x");
        InputStream input1 = new ByteArrayInputStream("x.y".getBytes());
        channel.basicPublish("amq.topic", "x.y", null, input1, input1.available());
        checkGet(Q1, true);
        checkGet(Q1, false);
        InputStream input2 = new ByteArrayInputStream("y.x".getBytes());
        channel.basicPublish("amq.topic", "y.x", null, input2, input2.available());
        checkGet(Q1, true);
        checkGet(Q1, false);
        InputStream input3 = new ByteArrayInputStream("x.x".getBytes());
        channel.basicPublish("amq.topic", "x.x", null, input3, input3.available());
        checkGet(Q1, true);
        checkGet(Q1, false);
    }

    @Test public void fanoutRouting() throws Exception {

        List<String> queues = new ArrayList<String>();

        for (int i = 0; i < 2; i++) {
            String q = "Q-" + System.nanoTime();
            channel.queueDeclare(q, false, true, true, null);
            channel.queueBind(q, "amq.fanout", "");
            queues.add(q);
        }

        InputStream input = new ByteArrayInputStream("fanout".getBytes());
        channel.basicPublish("amq.fanout", System.nanoTime() + "",
                             null, input, input.available());

        for (String q : queues) {
            checkGet(q, true);
        }

        for (String q : queues) {
            channel.queueDelete(q);
        }
    }

    @Test public void topicRouting() throws Exception {

        List<String> queues = new ArrayList<String>();

        //100+ queues is the trigger point for bug20046
        for (int i = 0; i < 100; i++) {
            channel.queueDeclare();
            AMQP.Queue.DeclareOk ok = channel.queueDeclare();
            String q = ok.getQueue();
            channel.queueBind(q, "amq.topic", "#");
            queues.add(q);
        }

        InputStream input = new ByteArrayInputStream("topic".getBytes());
        channel.basicPublish("amq.topic", "", null, input, input.available());

        for (String q : queues) {
            checkGet(q, true);
        }
    }

    @Test public void headersRouting() throws Exception {
        Map<String, Object> spec = new HashMap<String, Object>();
        spec.put("h1", "12345");
        spec.put("h2", "bar");
        spec.put("h3", null);
        spec.put("x-match", "all");
        channel.queueBind(Q1, "amq.match", "", spec);
        spec.put("x-match", "any");
        channel.queueBind(Q2, "amq.match", "", spec);

        AMQP.BasicProperties.Builder props = new AMQP.BasicProperties.Builder();

        InputStream input1 = new ByteArrayInputStream("0".getBytes());
        channel.basicPublish("amq.match", "", null, input1, input1.available());
        InputStream input2 = new ByteArrayInputStream("0b".getBytes());
        channel.basicPublish("amq.match", "", props.build(), input2, input2.available());

        Map<String, Object> map = new HashMap<String, Object>();
        props.headers(map);
        
        map.clear();
        map.put("h1", "12345");
        InputStream input3 = new ByteArrayInputStream("1".getBytes());
        channel.basicPublish("amq.match", "", props.build(), input3, input3.available());

        map.clear();
        map.put("h1", "12345");
        InputStream input4 = new ByteArrayInputStream("b".getBytes());
        channel.basicPublish("amq.match", "", props.build(), input4, input4.available());

        map.clear();
        map.put("h2", "bar");
        InputStream input5 = new ByteArrayInputStream("2".getBytes());
        channel.basicPublish("amq.match", "", props.build(), input5, input5.available());

        map.clear();
        map.put("h1", "12345");
        map.put("h2", "bar");
        InputStream input6 = new ByteArrayInputStream("3".getBytes());
        channel.basicPublish("amq.match", "", props.build(), input6, input6.available());

        map.clear();
        map.put("h1", "12345");
        map.put("h2", "bar");
        map.put("h3", null);
        InputStream input7 = new ByteArrayInputStream("4".getBytes());
        channel.basicPublish("amq.match", "", props.build(), input7, input7.available());

        map.clear();
        map.put("h1", "12345");
        map.put("h2", "quux");
        InputStream input8 = new ByteArrayInputStream("5".getBytes());
        channel.basicPublish("amq.match", "", props.build(), input8, input8.available());

        map.clear();
        map.put("h1", "zot");
        map.put("h2", "quux");
        map.put("h3", null);
        InputStream input9 = new ByteArrayInputStream("6".getBytes());
        channel.basicPublish("amq.match", "", props.build(), input9, input9.available());

        map.clear();
        map.put("h3", null);
        InputStream input10 = new ByteArrayInputStream("7".getBytes());
        channel.basicPublish("amq.match", "", props.build(), input10, input10.available());

        map.clear();
        map.put("h1", "zot");
        map.put("h2", "quux");
        InputStream input11 = new ByteArrayInputStream("8".getBytes());
        channel.basicPublish("amq.match", "", props.build(), input11, input11.available());

        checkGet(Q1, true); // 4
        checkGet(Q1, false);

        checkGet(Q2, true); // 1
        checkGet(Q2, true); // 2
        checkGet(Q2, true); // 3
        checkGet(Q2, true); // 4
        checkGet(Q2, true); // 5
        checkGet(Q2, true); // 6
        checkGet(Q2, true); // 7
        checkGet(Q2, true); // 8
        checkGet(Q2, false);
    }

    @Test public void basicReturn() throws IOException {
        channel.addReturnListener(makeReturnListener());
        returnCell = new BlockingCell<Integer>();

        //returned 'mandatory' publish
        InputStream input1 = new ByteArrayInputStream("mandatory1".getBytes());
        channel.basicPublish("", "unknown", true, false, null, input1, input1.available());
        checkReturn(AMQP.NO_ROUTE);

        //routed 'mandatory' publish
        InputStream input2 = new ByteArrayInputStream("mandatory2".getBytes());
        channel.basicPublish("", Q1, true, false, null, input2, input2.available());
        assertNotNull(channel.basicGet(Q1, true));

        //'immediate' publish
        InputStream input3 = new ByteArrayInputStream("immediate".getBytes());
        channel.basicPublish("", Q1, false, true, null, input3, input3.available());
        try {
            channel.basicQos(0); //flush
            fail("basic.publish{immediate=true} should not be supported");
        } catch (IOException ioe) {
            checkShutdownSignal(AMQP.NOT_IMPLEMENTED, ioe);
        } catch (AlreadyClosedException ioe) {
            checkShutdownSignal(AMQP.NOT_IMPLEMENTED, ioe);
        }
    }

    @Test public void basicReturnTransactional() throws IOException {
        channel.txSelect();
        channel.addReturnListener(makeReturnListener());
        returnCell = new BlockingCell<Integer>();

        //returned 'mandatory' publish
        InputStream input1 = new ByteArrayInputStream("mandatory1".getBytes());
        channel.basicPublish("", "unknown", true, false, null, input1, input1.available());
        try {
            returnCell.uninterruptibleGet(200);
            fail("basic.return issued prior to tx.commit");
        } catch (TimeoutException toe) {}
        channel.txCommit();
        checkReturn(AMQP.NO_ROUTE);

        //routed 'mandatory' publish
        InputStream input2 = new ByteArrayInputStream("mandatory2".getBytes());
        channel.basicPublish("", Q1, true, false, null, input2, input2.available());
        channel.txCommit();
        assertNotNull(channel.basicGet(Q1, true));

        //returned 'mandatory' publish when message is routable on
        //publish but not on commit
        InputStream input3 = new ByteArrayInputStream("mandatory2".getBytes());
        channel.basicPublish("", Q1, true, false, null, input3, input3.available());
        channel.queueDelete(Q1);
        channel.txCommit();
        checkReturn(AMQP.NO_ROUTE);
        channel.queueDeclare(Q1, false, false, false, null);
    }

    protected ReturnListener makeReturnListener() {
        return new ReturnListener() {
            public void handleReturn(int replyCode,
                                     String replyText,
                                     String exchange,
                                     String routingKey,
                                     AMQP.BasicProperties properties,
                                     InputStream body)
                throws IOException {
                Routing.this.returnCell.set(replyCode);
            }
        };
    }

    protected void checkReturn(int replyCode) {
        assertEquals((int)returnCell.uninterruptibleGet(), AMQP.NO_ROUTE);
        returnCell = new BlockingCell<Integer>();
    }

}
