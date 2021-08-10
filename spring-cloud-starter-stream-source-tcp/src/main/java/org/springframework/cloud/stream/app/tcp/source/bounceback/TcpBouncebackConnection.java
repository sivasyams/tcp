package org.springframework.cloud.stream.app.tcp.source.bounceback;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;

import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;

import org.springframework.context.ApplicationEventPublisher;
import org.springframework.core.serializer.Serializer;
import org.springframework.integration.ip.tcp.connection.NoListenerException;
import org.springframework.integration.ip.tcp.connection.TcpConnectionSupport;
import org.springframework.integration.ip.tcp.connection.TcpListener;
import org.springframework.integration.ip.tcp.serializer.SoftEndOfStreamException;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessagingException;
import org.springframework.scheduling.SchedulingAwareRunnable;

/**
 * The TcpBouncebackConnection class
 *
 * @author sivasyam
 *
 */
public class TcpBouncebackConnection extends TcpConnectionSupport implements SchedulingAwareRunnable {

    private final Socket socket;

    private volatile OutputStream socketOutputStream;

    private volatile long lastRead = System.currentTimeMillis();

    private volatile long lastSend;

    private Message<?> bounceBackMessage = null;

    public TcpBouncebackConnection(Socket socket, boolean server, boolean lookupHost, ApplicationEventPublisher applicationEventPublisher, String connectionFactoryName,
            Message<?> bounceBackMessage) {
        super(socket, server, lookupHost, applicationEventPublisher, connectionFactoryName);
        this.socket = socket;
        this.bounceBackMessage = bounceBackMessage;
    }

    @Override
    public boolean isLongLived() {
        return true;
    }

    @Override
    public void close() {
        this.setNoReadErrorOnClose(true);
        try {
            this.socket.close();
        } catch (Exception e) {
        }
        super.close();
    }

    @Override
    public boolean isOpen() {
        return !this.socket.isClosed();
    }

    @Override
    @SuppressWarnings("unchecked")
    public synchronized void send(Message<?> message) throws Exception {
        if (this.socketOutputStream == null) {
            int writeBufferSize = this.socket.getSendBufferSize();
            this.socketOutputStream = new BufferedOutputStream(this.socket.getOutputStream(), writeBufferSize > 0 ? writeBufferSize : 8192);
        }
        Object object = this.getMapper().fromMessage(message);
        this.lastSend = System.currentTimeMillis();
        try {
            ((Serializer<Object>) this.getSerializer()).serialize(object, this.socketOutputStream);
            this.socketOutputStream.flush();
        } catch (Exception e) {
            this.publishConnectionExceptionEvent(new MessagingException(message, "Failed TCP serialization", e));
            this.closeConnection(true);
            throw e;
        }
        if (logger.isDebugEnabled()) {
            logger.debug(getConnectionId() + " Message sent " + message);
        }
    }

    @Override
    public Object getPayload() throws Exception {
        return this.getDeserializer().deserialize(inputStream());
    }

    @Override
    public int getPort() {
        return this.socket.getPort();
    }

    @Override
    public Object getDeserializerStateKey() {
        try {
            return inputStream();
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public SSLSession getSslSession() {
        if (this.socket instanceof SSLSocket) {
            return ((SSLSocket) this.socket).getSession();
        } else {
            return null;
        }
    }

    protected InputStream inputStream() throws IOException {
        return this.socket.getInputStream();
    }

    @Override
    public void run() {
        boolean okToRun = true;
        if (logger.isDebugEnabled()) {
            logger.debug(this.getConnectionId() + " Reading...");
        }
        while (okToRun) {
            Message<?> message = null;
            try {
                message = this.getMapper().toMessage(this);
                this.lastRead = System.currentTimeMillis();
            } catch (Exception e) {
                this.publishConnectionExceptionEvent(e);
                if (handleReadException(e)) {
                    okToRun = false;
                }
            }
            if (okToRun && message != null) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Message received " + message);
                }
                try {
                    TcpListener listener = getListener();
                    if (listener == null) {
                        throw new NoListenerException("No listener");
                    }
                    listener.onMessage(message);
                    if (this.isServer() && bounceBackMessage != null) {
                        this.send(bounceBackMessage);
                    }
                } catch (NoListenerException nle) {
                    if (logger.isWarnEnabled()) {
                        logger.warn("Unexpected message - no endpoint registered with connection interceptor: " + getConnectionId() + " - " + message);
                    }
                } catch (Exception e2) {
                    logger.error("Exception sending message: " + message, e2);
                }
            }
        }
    }

    protected boolean handleReadException(Exception e) {
        boolean doClose = true;

        if (!this.isServer() && e instanceof SocketTimeoutException) {
            long now = System.currentTimeMillis();
            try {
                int soTimeout = this.socket.getSoTimeout();
                if (now - this.lastSend < soTimeout && now - this.lastRead < soTimeout * 2) {
                    doClose = false;
                }
                if (!doClose && logger.isDebugEnabled()) {
                    logger.debug("Skipping a socket timeout because we have a recent send " + this.getConnectionId());
                }
            } catch (SocketException e1) {
                logger.error("Error accessing soTimeout", e1);
            }
        }
        if (doClose) {
            boolean noReadErrorOnClose = this.isNoReadErrorOnClose();
            closeConnection(true);
            if (!(e instanceof SoftEndOfStreamException)) {
                if (e instanceof SocketTimeoutException) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Closed socket after timeout:" + this.getConnectionId());
                    }
                } else {
                    if (noReadErrorOnClose) {
                        if (logger.isTraceEnabled()) {
                            logger.trace("Read exception " + this.getConnectionId(), e);
                        } else if (logger.isDebugEnabled()) {
                            logger.debug("Read exception " + this.getConnectionId() + " " + e.getClass().getSimpleName() + ":" + (e.getCause() != null ? e.getCause() + ":" : "") + e.getMessage());
                        }
                    } else if (logger.isTraceEnabled()) {
                        logger.error("Read exception " + this.getConnectionId(), e);
                    } else {
                        logger.error("Read exception " + this.getConnectionId() + " " + e.getClass().getSimpleName() + ":" + (e.getCause() != null ? e.getCause() + ":" : "") + e.getMessage());
                    }
                }
                this.sendExceptionToListener(e);
            }
        }
        return doClose;
    }
}
