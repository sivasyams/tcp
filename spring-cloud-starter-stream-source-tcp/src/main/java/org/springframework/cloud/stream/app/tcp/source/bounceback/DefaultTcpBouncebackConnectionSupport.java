package org.springframework.cloud.stream.app.tcp.source.bounceback;

import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.net.Socket;

import org.springframework.context.ApplicationEventPublisher;
import org.springframework.integration.ip.tcp.connection.AbstractTcpConnectionSupport;
import org.springframework.messaging.Message;

/**
 * The BoucebackTcpNetConnectionSupport class
 *
 * @author sivasyam
 *
 */
public class DefaultTcpBouncebackConnectionSupport extends AbstractTcpConnectionSupport implements TcpBouncebackConnectionSupport {

    @Override
    public TcpBouncebackConnection createNewConnection(Socket socket, boolean server, boolean lookupHost, ApplicationEventPublisher applicationEventPublisher, String connectionFactoryName,
            Message<?> bouncebackMessage) throws Exception {
        if (isPushbackCapable()) {
            return new PushBackTcpBouncebackConnection(socket, server, lookupHost, applicationEventPublisher, connectionFactoryName, getPushbackBufferSize(), bouncebackMessage);
        } else {
            return new TcpBouncebackConnection(socket, server, lookupHost, applicationEventPublisher, connectionFactoryName, bouncebackMessage);
        }
    }

    private static final class PushBackTcpBouncebackConnection extends TcpBouncebackConnection {

        private final int pushbackBufferSize;

        private final String connectionId;

        private volatile PushbackInputStream pushbackStream;

        private volatile InputStream wrapped;

        PushBackTcpBouncebackConnection(Socket socket, boolean server, boolean lookupHost, ApplicationEventPublisher applicationEventPublisher, String connectionFactoryName, int bufferSize,
                Message<?> bouncebackMessage) {
            super(socket, server, lookupHost, applicationEventPublisher, connectionFactoryName, bouncebackMessage);
            this.pushbackBufferSize = bufferSize;
            this.connectionId = "pushback:" + super.getConnectionId();
        }

        @Override
        protected InputStream inputStream() throws IOException {
            InputStream wrapped = super.inputStream();
            if (this.pushbackStream == null || wrapped != this.wrapped) {
                this.pushbackStream = new PushbackInputStream(wrapped, this.pushbackBufferSize);
                this.wrapped = wrapped;
            }
            return this.pushbackStream;
        }

        @Override
        public String getConnectionId() {
            return this.connectionId;
        }

    }

}
