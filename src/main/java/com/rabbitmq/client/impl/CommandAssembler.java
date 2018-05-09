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

package com.rabbitmq.client.impl;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.UnexpectedFrameError;

import java.io.IOException;
import java.io.InputStream;

/**
 * Class responsible for piecing together a command from a series of {@link Frame}s.
 * <p/><b>Concurrency</b><br/>
 * This class is thread-safe, since all methods are synchronised. Callers should not
 * synchronise on objects of this class unless they are sole owners.
 * @see AMQCommand
 */
final class CommandAssembler {

    /** Current state, used to decide how to handle each incoming frame. */
    private enum CAState {
        EXPECTING_METHOD, EXPECTING_CONTENT_HEADER, EXPECTING_CONTENT_BODY, COMPLETE
    }
    private CAState state;

    /** The method for this command */
    private Method method;

    /** The content header for this command */
    private AMQContentHeader contentHeader;

    private final InputStream body;
    private VolatileFileOutputStream accumulator;
    /**
     * sum of the lengths of all fragments
     */
    private int bodyLength;

    /** No bytes of content body not yet accumulated */
    private long remainingBodyBytes;

    public CommandAssembler(Method method, AMQContentHeader contentHeader, InputStream body, int bodyLength) {
        this.method = method;
        this.contentHeader = contentHeader;
        this.bodyLength = bodyLength;
        this.remainingBodyBytes = 0;
        this.body = body;

        if (method == null) {
            this.state = CAState.EXPECTING_METHOD;
        } else if (contentHeader == null) {
            this.state = method.hasContent() ? CAState.EXPECTING_CONTENT_HEADER : CAState.COMPLETE;
        } else {
            this.remainingBodyBytes = contentHeader.getBodySize() - this.bodyLength;
            updateContentBodyState();
        }

        if (this.body != null) {
            return;
        }
        try {
            this.accumulator = new VolatileFileOutputStream();
        }
        catch (IOException e) {
            throw new UnsupportedOperationException();
        }
    }

    public synchronized Method getMethod() {
        return this.method;
    }

    public synchronized AMQContentHeader getContentHeader() {
        return this.contentHeader;
    }

    /** @return true if the command is complete */
    public synchronized boolean isComplete() {
        return (this.state == CAState.COMPLETE);
    }

    /** Decides whether more body frames are expected */
    private void updateContentBodyState() {
        this.state = (this.remainingBodyBytes > 0) ? CAState.EXPECTING_CONTENT_BODY : CAState.COMPLETE;
    }

    private void consumeMethodFrame(Frame f) throws IOException {
        if (f.type == AMQP.FRAME_METHOD) {
            this.method = AMQImpl.readMethodFrom(f.getInputStream());
            this.state = this.method.hasContent() ? CAState.EXPECTING_CONTENT_HEADER : CAState.COMPLETE;
        } else {
            throw new UnexpectedFrameError(f, AMQP.FRAME_METHOD);
        }
    }

    private void consumeHeaderFrame(Frame f) throws IOException {
        if (f.type == AMQP.FRAME_HEADER) {
            this.contentHeader = AMQImpl.readContentHeaderFrom(f.getInputStream());
            this.remainingBodyBytes = this.contentHeader.getBodySize();
            updateContentBodyState();
        } else {
            throw new UnexpectedFrameError(f, AMQP.FRAME_HEADER);
        }
    }

    private void consumeBodyFrame(Frame f) {
        if (f.type == AMQP.FRAME_BODY) {
            byte[] fragment = f.getPayload();
            this.remainingBodyBytes -= fragment.length;
            updateContentBodyState();
            if (this.remainingBodyBytes < 0) {
                throw new UnsupportedOperationException("%%%%%% FIXME unimplemented");
            }
            try {
                appendBodyFragment(fragment);
            }
            catch (IOException e) {
                //TODO update exception
                throw new UnsupportedOperationException("%%%%%% FIXME unimplemented");
            }
        }
        else {
            throw new UnexpectedFrameError(f, AMQP.FRAME_BODY);
        }
    }

    public synchronized InputStream getContentBody() throws IOException {
        if (body != null) {
            return body;
        }
        InputStream input = accumulator.getInputStream();
        accumulator.close();
        return input;
    }

    private void appendBodyFragment(byte[] fragment) throws IOException {
        if (fragment == null || fragment.length == 0) {
            return;
        }
        accumulator.write(fragment);
        bodyLength += fragment.length;
    }

    public int getBodyLength(){
        return bodyLength;
    }

    /**
     * @param f frame to be incorporated
     * @return true if command becomes complete
     * @throws IOException if error reading frame
     */
    public synchronized boolean handleFrame(Frame f) throws IOException
    {
        switch (this.state) {
          case EXPECTING_METHOD:          consumeMethodFrame(f); break;
          case EXPECTING_CONTENT_HEADER:  consumeHeaderFrame(f); break;
          case EXPECTING_CONTENT_BODY:    consumeBodyFrame(f);   break;

          default:
              throw new AssertionError("Bad Command State " + this.state);
        }
        return isComplete();
    }
}
