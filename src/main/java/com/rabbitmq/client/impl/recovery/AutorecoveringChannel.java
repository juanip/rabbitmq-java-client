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

package com.rabbitmq.client.impl.recovery;

import com.rabbitmq.client.*;
import com.rabbitmq.client.RecoverableChannel;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeoutException;

/**
 * {@link com.rabbitmq.client.Channel} implementation that is automatically
 * recovered during connection recovery.
 *
 * @since 3.3.0
 */
public class AutorecoveringChannel implements RecoverableChannel {
    private volatile RecoveryAwareChannelN delegate;
    private volatile AutorecoveringConnection connection;
    private final List<ShutdownListener> shutdownHooks  = new CopyOnWriteArrayList<ShutdownListener>();
    private final List<RecoveryListener> recoveryListeners = new CopyOnWriteArrayList<RecoveryListener>();
    private final List<ReturnListener> returnListeners = new CopyOnWriteArrayList<ReturnListener>();
    private final List<ConfirmListener> confirmListeners = new CopyOnWriteArrayList<ConfirmListener>();
    private final List<FlowListener> flowListeners = new CopyOnWriteArrayList<FlowListener>();
    private final Set<String> consumerTags = Collections.synchronizedSet(new HashSet<String>());
    private int prefetchCountConsumer;
    private int prefetchCountGlobal;
    private boolean usesPublisherConfirms;
    private boolean usesTransactions;

    public AutorecoveringChannel(AutorecoveringConnection connection, RecoveryAwareChannelN delegate) {
        this.connection = connection;
        this.delegate = delegate;
    }

    @Override
    public int getChannelNumber() {
        return delegate.getChannelNumber();
    }

    @Override
    public Connection getConnection() {
        return delegate.getConnection();
    }

    public Channel getDelegate() {
        return delegate;
    }

    @Override
    public void close() throws IOException {
        try {
            delegate.close();
        } finally {
            for (String consumerTag : consumerTags) {
                this.connection.deleteRecordedConsumer(consumerTag);
            }
            this.connection.unregisterChannel(this);
        }
    }

    @Override
    public void close(int closeCode, String closeMessage) throws IOException {
        try {
          delegate.close(closeCode, closeMessage);
        } finally {
          this.connection.unregisterChannel(this);
        }
    }

    @Override
    @Deprecated
    public boolean flowBlocked() {
        return delegate.flowBlocked();
    }

    @Override
    public void abort() throws IOException {
        delegate.abort();
    }

    @Override
    public void abort(int closeCode, String closeMessage) throws IOException {
        delegate.abort(closeCode, closeMessage);
    }

    @Override
    public void addReturnListener(ReturnListener listener) {
        this.returnListeners.add(listener);
        delegate.addReturnListener(listener);
    }

    @Override
    public boolean removeReturnListener(ReturnListener listener) {
        this.returnListeners.remove(listener);
        return delegate.removeReturnListener(listener);
    }

    @Override
    public void clearReturnListeners() {
        this.returnListeners.clear();
        delegate.clearReturnListeners();
    }

    @Override
    @Deprecated
    public void addFlowListener(FlowListener listener) {
        this.flowListeners.add(listener);
        delegate.addFlowListener(listener);
    }

    @Override
    @Deprecated
    public boolean removeFlowListener(FlowListener listener) {
        this.flowListeners.remove(listener);
        return delegate.removeFlowListener(listener);
    }

    @Override
    @Deprecated
    public void clearFlowListeners() {
        this.flowListeners.clear();
        delegate.clearFlowListeners();
    }

    @Override
    public void addConfirmListener(ConfirmListener listener) {
        this.confirmListeners.add(listener);
        delegate.addConfirmListener(listener);
    }

    @Override
    public boolean removeConfirmListener(ConfirmListener listener) {
        this.confirmListeners.remove(listener);
        return delegate.removeConfirmListener(listener);
    }

    @Override
    public void clearConfirmListeners() {
        this.confirmListeners.clear();
        delegate.clearConfirmListeners();
    }

    @Override
    public Consumer getDefaultConsumer() {
        return delegate.getDefaultConsumer();
    }

    @Override
    public void setDefaultConsumer(Consumer consumer) {
        delegate.setDefaultConsumer(consumer);
    }

    @Override
    public void basicQos(int prefetchSize, int prefetchCount, boolean global) throws IOException {
        if (global) {
            this.prefetchCountGlobal = prefetchCount;
        } else {
            this.prefetchCountConsumer = prefetchCount;
        }

        delegate.basicQos(prefetchSize, prefetchCount, global);
    }

    @Override
    public void basicQos(int prefetchCount) throws IOException {
        basicQos(0, prefetchCount, false);
    }

    @Override
    public void basicQos(int prefetchCount, boolean global) throws IOException {
        basicQos(0, prefetchCount, global);
    }

    @Override
    public void basicPublish(String exchange, String routingKey, AMQP.BasicProperties props, byte[] body) throws IOException {
        delegate.basicPublish(exchange, routingKey, props, body);
    }

    @Override
    public void basicPublish(String exchange, String routingKey, boolean mandatory, AMQP.BasicProperties props, byte[] body) throws IOException {
        delegate.basicPublish(exchange, routingKey, mandatory, props, body);
    }

    @Override
    public void basicPublish(String exchange, String routingKey, boolean mandatory, boolean immediate, AMQP.BasicProperties props, byte[] body) throws IOException {
        delegate.basicPublish(exchange, routingKey, mandatory, immediate, props, body);
    }

    @Override
    public void basicPublish(String exchange, String routingKey, boolean mandatory, boolean immediate, AMQP.BasicProperties props, InputStream body, int bodyLength) throws IOException {
        delegate.basicPublish(exchange, routingKey, mandatory, immediate, props, body, bodyLength);
    }

    @Override
    public AMQP.Exchange.DeclareOk exchangeDeclare(String exchange, String type) throws IOException {
        return exchangeDeclare(exchange, type, false, false, null);
    }

    @Override
    public AMQP.Exchange.DeclareOk exchangeDeclare(String exchange, BuiltinExchangeType type) throws IOException {
        return exchangeDeclare(exchange, type.getType());
    }

    @Override
    public AMQP.Exchange.DeclareOk exchangeDeclare(String exchange, String type, boolean durable) throws IOException {
        return exchangeDeclare(exchange, type, durable, false, null);
    }

    @Override
    public AMQP.Exchange.DeclareOk exchangeDeclare(String exchange, BuiltinExchangeType type, boolean durable) throws IOException {
        return exchangeDeclare(exchange, type.getType(), durable);
    }

    @Override
    public AMQP.Exchange.DeclareOk exchangeDeclare(String exchange, String type, boolean durable, boolean autoDelete, Map<String, Object> arguments) throws IOException {
        return exchangeDeclare(exchange, type, durable, autoDelete, false, arguments);
    }

    @Override
    public AMQP.Exchange.DeclareOk exchangeDeclare(String exchange, BuiltinExchangeType type, boolean durable, boolean autoDelete, Map<String, Object> arguments) throws IOException {
        return exchangeDeclare(exchange, type.getType(), durable, autoDelete, arguments);
    }

    @Override
    public AMQP.Exchange.DeclareOk exchangeDeclare(String exchange, String type, boolean durable, boolean autoDelete, boolean internal, Map<String, Object> arguments) throws IOException {
        final AMQP.Exchange.DeclareOk ok = delegate.exchangeDeclare(exchange, type, durable, autoDelete, internal, arguments);
        RecordedExchange x = new RecordedExchange(this, exchange).
          type(type).
          durable(durable).
          autoDelete(autoDelete).
          arguments(arguments);
        recordExchange(exchange, x);
        return ok;
    }

    @Override
    public AMQP.Exchange.DeclareOk exchangeDeclare(String exchange, BuiltinExchangeType type, boolean durable, boolean autoDelete, boolean internal, Map<String, Object> arguments) throws IOException {
        return exchangeDeclare(exchange, type.getType(), durable, autoDelete, internal, arguments);
    }

    @Override
    public void exchangeDeclareNoWait(String exchange, String type, boolean durable, boolean autoDelete, boolean internal, Map<String, Object> arguments) throws IOException {
        RecordedExchange x = new RecordedExchange(this, exchange).
          type(type).
          durable(durable).
          autoDelete(autoDelete).
          arguments(arguments);
        recordExchange(exchange, x);
        delegate.exchangeDeclareNoWait(exchange, type, durable, autoDelete, internal, arguments);
    }

    @Override
    public void exchangeDeclareNoWait(String exchange, BuiltinExchangeType type, boolean durable, boolean autoDelete, boolean internal, Map<String, Object> arguments) throws IOException {
        exchangeDeclareNoWait(exchange, type.getType(), durable, autoDelete, internal, arguments);
    }

    @Override
    public AMQP.Exchange.DeclareOk exchangeDeclarePassive(String name) throws IOException {
        return delegate.exchangeDeclarePassive(name);
    }

    @Override
    public AMQP.Exchange.DeleteOk exchangeDelete(String exchange, boolean ifUnused) throws IOException {
        deleteRecordedExchange(exchange);
        return delegate.exchangeDelete(exchange, ifUnused);
    }

    @Override
    public void exchangeDeleteNoWait(String exchange, boolean ifUnused) throws IOException {
        deleteRecordedExchange(exchange);
        delegate.exchangeDeleteNoWait(exchange, ifUnused);
    }

    @Override
    public AMQP.Exchange.DeleteOk exchangeDelete(String exchange) throws IOException {
        return exchangeDelete(exchange, false);
    }

    @Override
    public AMQP.Exchange.BindOk exchangeBind(String destination, String source, String routingKey) throws IOException {
        return exchangeBind(destination, source, routingKey, null);
    }

    @Override
    public AMQP.Exchange.BindOk exchangeBind(String destination, String source, String routingKey, Map<String, Object> arguments) throws IOException {
        final AMQP.Exchange.BindOk ok = delegate.exchangeBind(destination, source, routingKey, arguments);
        recordExchangeBinding(destination, source, routingKey, arguments);
        return ok;
    }

    @Override
    public void exchangeBindNoWait(String destination, String source, String routingKey, Map<String, Object> arguments) throws IOException {
        delegate.exchangeBindNoWait(destination, source, routingKey, arguments);
        recordExchangeBinding(destination, source, routingKey, arguments);
    }

    @Override
    public AMQP.Exchange.UnbindOk exchangeUnbind(String destination, String source, String routingKey) throws IOException {
        return exchangeUnbind(destination, source, routingKey, null);
    }

    @Override
    public AMQP.Exchange.UnbindOk exchangeUnbind(String destination, String source, String routingKey, Map<String, Object> arguments) throws IOException {
        deleteRecordedExchangeBinding(destination, source, routingKey, arguments);
        this.maybeDeleteRecordedAutoDeleteExchange(source);
        return delegate.exchangeUnbind(destination, source, routingKey, arguments);
    }

    @Override
    public void exchangeUnbindNoWait(String destination, String source, String routingKey, Map<String, Object> arguments) throws IOException {
        delegate.exchangeUnbindNoWait(destination, source, routingKey, arguments);
        deleteRecordedExchangeBinding(destination, source, routingKey, arguments);
    }

    @Override
    public AMQP.Queue.DeclareOk queueDeclare() throws IOException {
        return queueDeclare("", false, true, true, null);
    }

    @Override
    public AMQP.Queue.DeclareOk queueDeclare(String queue, boolean durable, boolean exclusive, boolean autoDelete, Map<String, Object> arguments) throws IOException {
        final AMQP.Queue.DeclareOk ok = delegate.queueDeclare(queue, durable, exclusive, autoDelete, arguments);
        RecordedQueue q = new RecordedQueue(this, ok.getQueue()).
            durable(durable).
            exclusive(exclusive).
            autoDelete(autoDelete).
            arguments(arguments);
        if (queue.equals(RecordedQueue.EMPTY_STRING)) {
            q.serverNamed(true);
        }
        recordQueue(ok, q);
        return ok;
    }

    @Override
    public void queueDeclareNoWait(String queue,
                                   boolean durable,
                                   boolean exclusive,
                                   boolean autoDelete,
                                   Map<String, Object> arguments) throws IOException {
        RecordedQueue meta = new RecordedQueue(this, queue).
            durable(durable).
            exclusive(exclusive).
            autoDelete(autoDelete).
            arguments(arguments);
        delegate.queueDeclareNoWait(queue, durable, exclusive, autoDelete, arguments);
        recordQueue(queue, meta);

    }

    @Override
    public AMQP.Queue.DeclareOk queueDeclarePassive(String queue) throws IOException {
        return delegate.queueDeclarePassive(queue);
    }

    @Override
    public long messageCount(String queue) throws IOException {
        return delegate.messageCount(queue);
    }

    @Override
    public long consumerCount(String queue) throws IOException {
        return delegate.consumerCount(queue);
    }

    @Override
    public AMQP.Queue.DeleteOk queueDelete(String queue) throws IOException {
        return queueDelete(queue, false, false);
    }

    @Override
    public AMQP.Queue.DeleteOk queueDelete(String queue, boolean ifUnused, boolean ifEmpty) throws IOException {
        deleteRecordedQueue(queue);
        return delegate.queueDelete(queue, ifUnused, ifEmpty);
    }

    @Override
    public void queueDeleteNoWait(String queue, boolean ifUnused, boolean ifEmpty) throws IOException {
        deleteRecordedQueue(queue);
        delegate.queueDeleteNoWait(queue, ifUnused, ifEmpty);
    }

    @Override
    public AMQP.Queue.BindOk queueBind(String queue, String exchange, String routingKey) throws IOException {
        return queueBind(queue, exchange, routingKey, null);
    }

    @Override
    public AMQP.Queue.BindOk queueBind(String queue, String exchange, String routingKey, Map<String, Object> arguments) throws IOException {
        AMQP.Queue.BindOk ok = delegate.queueBind(queue, exchange, routingKey, arguments);
        recordQueueBinding(queue, exchange, routingKey, arguments);
        return ok;
    }

    @Override
    public void queueBindNoWait(String queue, String exchange, String routingKey, Map<String, Object> arguments) throws IOException {
        delegate.queueBindNoWait(queue, exchange, routingKey, arguments);
        recordQueueBinding(queue, exchange, routingKey, arguments);
    }

    @Override
    public AMQP.Queue.UnbindOk queueUnbind(String queue, String exchange, String routingKey) throws IOException {
        return queueUnbind(queue, exchange, routingKey, null);
    }

    @Override
    public AMQP.Queue.UnbindOk queueUnbind(String queue, String exchange, String routingKey, Map<String, Object> arguments) throws IOException {
        deleteRecordedQueueBinding(queue, exchange, routingKey, arguments);
        this.maybeDeleteRecordedAutoDeleteExchange(exchange);
        return delegate.queueUnbind(queue, exchange, routingKey, arguments);
    }

    @Override
    public AMQP.Queue.PurgeOk queuePurge(String queue) throws IOException {
        return delegate.queuePurge(queue);
    }

    @Override
    public GetResponse basicGet(String queue, boolean autoAck) throws IOException {
        return delegate.basicGet(queue, autoAck);
    }

    @Override
    public StreamGetResponse basicStreamGet(String queue, boolean autoAck) throws IOException {
        return delegate.basicStreamGet(queue, autoAck);
    }


    @Override
    public void basicAck(long deliveryTag, boolean multiple) throws IOException {
        delegate.basicAck(deliveryTag, multiple);
    }

    @Override
    public void basicNack(long deliveryTag, boolean multiple, boolean requeue) throws IOException {
        delegate.basicNack(deliveryTag, multiple, requeue);
    }

    @Override
    public void basicReject(long deliveryTag, boolean requeue) throws IOException {
        delegate.basicReject(deliveryTag, requeue);
    }

    @Override
    public String basicConsume(String queue, Consumer callback) throws IOException {
        return basicConsume(queue, false, callback);
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, Consumer callback) throws IOException {
        return basicConsume(queue, autoAck, "", callback);
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, String consumerTag, Consumer callback) throws IOException {
        return basicConsume(queue, autoAck, consumerTag, false, false, null, callback);
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, Map<String, Object> arguments, Consumer callback) throws IOException {
        return basicConsume(queue, autoAck, "", false, false, arguments, callback);
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, String consumerTag, boolean noLocal, boolean exclusive, Map<String, Object> arguments, Consumer callback) throws IOException {
        final String result = delegate.basicConsume(queue, autoAck, consumerTag, noLocal, exclusive, arguments, callback);
        recordConsumer(result, queue, autoAck, exclusive, arguments, callback);
        return result;
    }

    @Override
    public void basicCancel(String consumerTag) throws IOException {
        RecordedConsumer c = this.deleteRecordedConsumer(consumerTag);
        if(c != null) {
            this.maybeDeleteRecordedAutoDeleteQueue(c.getQueue());
        }
        delegate.basicCancel(consumerTag);
    }

    @Override
    public AMQP.Basic.RecoverOk basicRecover() throws IOException {
        return delegate.basicRecover();
    }

    @Override
    public AMQP.Basic.RecoverOk basicRecover(boolean requeue) throws IOException {
        return delegate.basicRecover(requeue);
    }

    @Override
    public AMQP.Tx.SelectOk txSelect() throws IOException {
        this.usesTransactions = true;
        return delegate.txSelect();
    }

    @Override
    public AMQP.Tx.CommitOk txCommit() throws IOException {
        return delegate.txCommit();
    }

    @Override
    public AMQP.Tx.RollbackOk txRollback() throws IOException {
        return delegate.txRollback();
    }

    @Override
    public AMQP.Confirm.SelectOk confirmSelect() throws IOException {
        this.usesPublisherConfirms = true;
        return delegate.confirmSelect();
    }

    @Override
    public long getNextPublishSeqNo() {
        return delegate.getNextPublishSeqNo();
    }

    @Override
    public boolean waitForConfirms() throws InterruptedException {
        return delegate.waitForConfirms();
    }

    @Override
    public boolean waitForConfirms(long timeout) throws InterruptedException, TimeoutException {
        return delegate.waitForConfirms(timeout);
    }

    @Override
    public void waitForConfirmsOrDie() throws IOException, InterruptedException {
        delegate.waitForConfirmsOrDie();
    }

    @Override
    public void waitForConfirmsOrDie(long timeout) throws IOException, InterruptedException, TimeoutException {
        delegate.waitForConfirmsOrDie(timeout);
    }

    @Override
    public void asyncRpc(Method method) throws IOException {
        delegate.asyncRpc(method);
    }

    @Override
    public Command rpc(Method method) throws IOException {
        return delegate.rpc(method);
    }

    /**
     * @see Connection#addShutdownListener(com.rabbitmq.client.ShutdownListener)
     */
    @Override
    public void addShutdownListener(ShutdownListener listener) {
        this.shutdownHooks.add(listener);
        delegate.addShutdownListener(listener);
    }

    @Override
    public void removeShutdownListener(ShutdownListener listener) {
        this.shutdownHooks.remove(listener);
        delegate.removeShutdownListener(listener);
    }

    @Override
    public ShutdownSignalException getCloseReason() {
        return delegate.getCloseReason();
    }

    @Override
    public void notifyListeners() {
        delegate.notifyListeners();
    }

    @Override
    public boolean isOpen() {
        return delegate.isOpen();
    }

    @Override
    public void addRecoveryListener(RecoveryListener listener) {
        this.recoveryListeners.add(listener);
    }

    @Override
    public void removeRecoveryListener(RecoveryListener listener) {
        this.recoveryListeners.remove(listener);
    }

    //
    // Recovery
    //

    public void automaticallyRecover(AutorecoveringConnection connection, Connection connDelegate) throws IOException {
        RecoveryAwareChannelN defunctChannel = this.delegate;
        this.connection = connection;

        final RecoveryAwareChannelN newChannel = (RecoveryAwareChannelN) connDelegate.createChannel(this.getChannelNumber());
        if (newChannel == null)
            throw new IOException("Failed to create new channel for channel number=" + this.getChannelNumber() + " during recovery");
        newChannel.inheritOffsetFrom(defunctChannel);
        this.delegate = newChannel;

        this.notifyRecoveryListenersStarted();
        this.recoverShutdownListeners();
        this.recoverReturnListeners();
        this.recoverConfirmListeners();
        this.recoverFlowListeners();
        this.recoverState();
        this.notifyRecoveryListenersComplete();
    }

    private void recoverShutdownListeners() {
        for (ShutdownListener sh : this.shutdownHooks) {
            this.delegate.addShutdownListener(sh);
        }
    }

    private void recoverReturnListeners() {
        for(ReturnListener rl : this.returnListeners) {
            this.delegate.addReturnListener(rl);
        }
    }

    private void recoverConfirmListeners() {
        for(ConfirmListener cl : this.confirmListeners) {
            this.delegate.addConfirmListener(cl);
        }
    }

    @Deprecated
    private void recoverFlowListeners() {
        for(FlowListener fl : this.flowListeners) {
            this.delegate.addFlowListener(fl);
        }
    }

    private void recoverState() throws IOException {
        if (this.prefetchCountConsumer != 0) {
            basicQos(this.prefetchCountConsumer, false);
        }
        if (this.prefetchCountGlobal != 0) {
            basicQos(this.prefetchCountGlobal, true);
        }
        if(this.usesPublisherConfirms) {
            this.confirmSelect();
        }
        if(this.usesTransactions) {
            this.txSelect();
        }
    }

    private void notifyRecoveryListenersComplete() {
        for (RecoveryListener f : this.recoveryListeners) {
            f.handleRecovery(this);
        }
    }

    private void notifyRecoveryListenersStarted() {
        for (RecoveryListener f : this.recoveryListeners) {
            f.handleRecoveryStarted(this);
        }
    }

    private void recordQueueBinding(String queue, String exchange, String routingKey, Map<String, Object> arguments) {
        this.connection.recordQueueBinding(this, queue, exchange, routingKey, arguments);
    }

    private boolean deleteRecordedQueueBinding(String queue, String exchange, String routingKey, Map<String, Object> arguments) {
        return this.connection.deleteRecordedQueueBinding(this, queue, exchange, routingKey, arguments);
    }

    private void recordExchangeBinding(String destination, String source, String routingKey, Map<String, Object> arguments) {
        this.connection.recordExchangeBinding(this, destination, source, routingKey, arguments);
    }

    private boolean deleteRecordedExchangeBinding(String destination, String source, String routingKey, Map<String, Object> arguments) {
        return this.connection.deleteRecordedExchangeBinding(this, destination, source, routingKey, arguments);
    }

    private void recordQueue(AMQP.Queue.DeclareOk ok, RecordedQueue q) {
        this.connection.recordQueue(ok, q);
    }

    private void recordQueue(String queue, RecordedQueue meta) {
        this.connection.recordQueue(queue, meta);
    }

    private void deleteRecordedQueue(String queue) {
        this.connection.deleteRecordedQueue(queue);
    }

    private void recordExchange(String exchange, RecordedExchange x) {
        this.connection.recordExchange(exchange, x);
    }

    private void deleteRecordedExchange(String exchange) {
        this.connection.deleteRecordedExchange(exchange);
    }

    private void recordConsumer(String result,
                                String queue,
                                boolean autoAck,
                                boolean exclusive,
                                Map<String, Object> arguments,
                                Consumer callback) {
        RecordedConsumer consumer = new RecordedConsumer(this, queue).
                                            autoAck(autoAck).
                                            consumerTag(result).
                                            exclusive(exclusive).
                                            arguments(arguments).
                                            consumer(callback);
        this.consumerTags.add(result);
        this.connection.recordConsumer(result, consumer);
    }

    private RecordedConsumer deleteRecordedConsumer(String consumerTag) {
        this.consumerTags.remove(consumerTag);
        return this.connection.deleteRecordedConsumer(consumerTag);
    }

    private void maybeDeleteRecordedAutoDeleteQueue(String queue) {
        this.connection.maybeDeleteRecordedAutoDeleteQueue(queue);
    }

    private void maybeDeleteRecordedAutoDeleteExchange(String exchange) {
        this.connection.maybeDeleteRecordedAutoDeleteExchange(exchange);
    }

    void updateConsumerTag(String tag, String newTag) {
        synchronized (this.consumerTags) {
            consumerTags.remove(tag);
            consumerTags.add(newTag);
        }
    }

    @Override
    public String toString() {
        return this.delegate.toString();
    }

}
