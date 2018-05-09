package com.rabbitmq.client;

import com.rabbitmq.client.AMQP.BasicProperties;

import java.io.InputStream;

/**
 * Encapsulates the response from a {@link Channel#basicGet} message-retrieval method call
 * - essentially a static bean "holder" with message response data.
 */
public class StreamGetResponse {
    private final Envelope envelope;
    private final BasicProperties props;
    private final InputStream body;
    private final int messageCount;

    /**
     * Construct a {@link StreamGetResponse} with the specified construction parameters
     * @param envelope the {@link Envelope}
     * @param props message properties
     * @param body the message body
     * @param messageCount the server's most recent estimate of the number of messages remaining on the queue
     */
    public StreamGetResponse(Envelope envelope, BasicProperties props, InputStream body, int messageCount)
    {
        this.envelope = envelope;
        this.props = props;
        this.body = body;
        this.messageCount = messageCount;
    }

    /**
     * Get the {@link Envelope} included in this response
     * @return the envelope
     */
    public Envelope getEnvelope() {
        return envelope;
    }

    /**
     * Get the {@link BasicProperties} included in this response
     * @return the properties
     */
    public BasicProperties getProps() {
        return props;
    }

    /**
     * Get the message body included in this response
     * @return the message body
     */
    public InputStream getBody() {
        return body;
    }

    /**
     * Get the server's most recent estimate of the number of messages
     * remaining on the queue. This number can only ever be a rough
     * estimate, because of concurrent activity at the server and the
     * delay between the server sending its estimate and the client
     * receiving and processing the message containing the estimate.
     *
     * <p>According to the AMQP specification, this figure does not
     * include the message being delivered. For example, this field
     * will be zero in the simplest case of a single reader issuing a
     * Basic.Get on a private queue holding a single message (the
     * message being delivered in this StreamGetResponse).
     *
     * @return an estimate of the number of messages remaining to be
     * read from the queue
     */
    public int getMessageCount() {
        return messageCount;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("StreamGetResponse(envelope=").append(envelope);
        sb.append(", props=").append(props);
        sb.append(", messageCount=").append(messageCount);
        sb.append(")");
        return sb.toString();
    }
}
