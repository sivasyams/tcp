package org.springframework.cloud.stream.app.tcp.source.bounceback;

import java.net.Socket;

import org.springframework.context.ApplicationEventPublisher;
import org.springframework.messaging.Message;

/**
 * The TcpBouncebackConnectionSupport class
 *
 * @author sivasyam
 *
 */
public interface TcpBouncebackConnectionSupport {

    TcpBouncebackConnection createNewConnection(Socket socket, boolean server, boolean lookupHost, ApplicationEventPublisher applicationEventPublisher, String connectionFactoryName,
            Message<?> bouncebackMessage) throws Exception;
}
