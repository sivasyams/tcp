package org.springframework.cloud.stream.app.tcp.source.bounceback;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;

import javax.net.ServerSocketFactory;

import org.springframework.integration.ip.tcp.connection.AbstractServerConnectionFactory;
import org.springframework.integration.ip.tcp.connection.DefaultTcpNetSocketFactorySupport;
import org.springframework.integration.ip.tcp.connection.TcpConnectionEvent;
import org.springframework.integration.ip.tcp.connection.TcpConnectionOpenEvent;
import org.springframework.integration.ip.tcp.connection.TcpConnectionSupport;
import org.springframework.integration.ip.tcp.connection.TcpSocketFactorySupport;
import org.springframework.messaging.Message;
import org.springframework.util.Assert;

/**
 * The TcpBouncebackServerConnectionFactory class
 *
 * @author sivasyam
 *
 */
public class TcpBouncebackServerConnectionFactory extends AbstractServerConnectionFactory {

    private volatile ServerSocket serverSocket;

    private volatile TcpSocketFactorySupport tcpSocketFactorySupport = new DefaultTcpNetSocketFactorySupport();

    private TcpBouncebackConnectionSupport tcpBounceBackConnectionSupport = new DefaultTcpBouncebackConnectionSupport();

    private Message<?> bounceBackMessage = null;

    public TcpBouncebackServerConnectionFactory(int port, Message<?> bounceBackMessage) {
        super(port);
        this.bounceBackMessage = bounceBackMessage;
    }

    @Override
    public String getComponentType() {
        return "tcp-bounceback-server-connection-factory";
    }

    @Override
    public int getPort() {
        int port = super.getPort();
        ServerSocket serverSocket = this.serverSocket;
        if (port == 0 && serverSocket != null) {
            port = serverSocket.getLocalPort();
        }
        return port;
    }

    @Override
    public SocketAddress getServerSocketAddress() {
        if (this.serverSocket != null) {
            return this.serverSocket.getLocalSocketAddress();
        } else {
            return null;
        }
    }

    public void setTcpNetConnectionSupport(TcpBouncebackConnectionSupport connectionSupport) {
        Assert.notNull(connectionSupport, "'connectionSupport' cannot be null");
        this.tcpBounceBackConnectionSupport = connectionSupport;
    }

    @Override
    public void run() {
        ServerSocket theServerSocket = null;
        if (getListener() == null) {
            logger.info(this + " No listener bound to server connection factory; will not read; exiting...");
            return;
        }
        try {
            if (getLocalAddress() == null) {
                theServerSocket = createServerSocket(super.getPort(), getBacklog(), null);
            } else {
                InetAddress whichNic = InetAddress.getByName(getLocalAddress());
                theServerSocket = createServerSocket(super.getPort(), getBacklog(), whichNic);
            }
            getTcpSocketSupport().postProcessServerSocket(theServerSocket);
            this.serverSocket = theServerSocket;
            setListening(true);
            logger.info(this + " Listening");
            publishServerListeningEvent(getPort());
            while (true) {
                final Socket socket;

                try {
                    if (this.serverSocket == null) {
                        if (logger.isDebugEnabled()) {
                            logger.debug(this + " stopped before accept");
                        }
                        throw new IOException(this + " stopped before accept");
                    } else {
                        socket = this.serverSocket.accept();
                    }
                } catch (SocketTimeoutException ste) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Timed out on accept; continuing");
                    }
                    continue;
                }
                if (isShuttingDown()) {
                    if (logger.isInfoEnabled()) {
                        logger.info("New connection from " + socket.getInetAddress().getHostAddress() + ":" + socket.getPort() + " rejected; the server is in the process of shutting down.");
                    }
                    socket.close();
                } else {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Accepted connection from " + socket.getInetAddress().getHostAddress() + ":" + socket.getPort());
                    }
                    try {
                        setSocketAttributes(socket);
                        TcpConnectionSupport connection = this.tcpBounceBackConnectionSupport.createNewConnection(socket, true, isLookupHost(), getApplicationEventPublisher(), getComponentName(),
                                bounceBackMessage);
                        connection = wrapConnection(connection);
                        initializeConnection(connection, socket);
                        getTaskExecutor().execute(connection);
                        harvestClosedConnections();
                        getComponentName();
                        TcpConnectionEvent event = new TcpConnectionOpenEvent(connection, getComponentName());
                        connection.publishEvent(event);
                    } catch (Exception e) {
                        this.logger.error("Failed to create and configure a TcpConnection for the new socket: " + socket.getInetAddress().getHostAddress() + ":" + socket.getPort(), e);
                        try {
                            socket.close();
                        } catch (IOException e1) {

                        }
                    }
                }
            }
        } catch (Exception e) {
            if (e instanceof SocketException && theServerSocket != null) {
                logger.info("Server Socket closed");
            } else if (isActive()) {
                logger.error("Error on ServerSocket; port = " + getPort(), e);
                publishServerExceptionEvent(e);
                stop();
            }
        } finally {
            setListening(false);
            setActive(false);
        }
    }

    /**
     * Create a new {@link ServerSocket}. This default implementation uses the default {@link ServerSocketFactory}. Override to use some other mechanism
     * 
     * @param port
     *            The port.
     * @param backlog
     *            The server socket backlog.
     * @param whichNic
     *            An InetAddress if binding to a specific network interface. Set to null when configured to bind to all interfaces.
     * @return The Server Socket.
     * @throws IOException
     *             Any IOException.
     */
    protected ServerSocket createServerSocket(int port, int backlog, InetAddress whichNic) throws IOException {
        ServerSocketFactory serverSocketFactory = this.tcpSocketFactorySupport.getServerSocketFactory();
        if (whichNic == null) {
            return serverSocketFactory.createServerSocket(port, Math.abs(backlog));
        } else {
            return serverSocketFactory.createServerSocket(port, Math.abs(backlog), whichNic);
        }
    }

    @Override
    public void stop() {
        if (this.serverSocket == null) {
            return;
        }
        try {
            this.serverSocket.close();
        } catch (IOException e) {
        }
        this.serverSocket = null;
        super.stop();
    }

    protected ServerSocket getServerSocket() {
        return this.serverSocket;
    }

    protected TcpSocketFactorySupport getTcpSocketFactorySupport() {
        return this.tcpSocketFactorySupport;
    }

    public void setTcpSocketFactorySupport(TcpSocketFactorySupport tcpSocketFactorySupport) {
        Assert.notNull(tcpSocketFactorySupport, "TcpSocketFactorySupport may not be null");
        this.tcpSocketFactorySupport = tcpSocketFactorySupport;
    }
}