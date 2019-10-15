package org.apache.dubbo.remoting.transport.xsocket;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.remoting.ChannelHandler;
import org.apache.dubbo.remoting.Codec2;
import org.apache.dubbo.remoting.RemotingException;
import org.xsocket.connection.IDataHandler;
import org.xsocket.connection.IConnectHandler;
import org.xsocket.connection.IDisconnectHandler;
import org.xsocket.connection.IConnectExceptionHandler;
import org.xsocket.connection.IConnectionTimeoutHandler;
import org.xsocket.connection.INonBlockingConnection;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.util.concurrent.CompletableFuture;


public class XsocketClientHander implements IDataHandler, IConnectHandler, IDisconnectHandler, IConnectExceptionHandler,  IConnectionTimeoutHandler {
    private static final Logger logger = LoggerFactory.getLogger(XsocketServer.class);

    private CompletableFuture<Boolean> future;

    private final URL url;

    private final ChannelHandler handler;


    private final Codec2 codec;


    XsocketClientHander(CompletableFuture<Boolean> future, URL url, ChannelHandler handler, Codec2 codec) {
        this.future = future;
        this.url = url;
        this.handler = handler;
        this.codec = codec;
    }


    @Override
    public boolean onConnectException(INonBlockingConnection connection, IOException ioe)  {
        XsocketChannel channel = XsocketChannel.getOrAddChannel(connection, url, handler, codec);
        try {
            if (!future.isDone()) {
                future.completeExceptionally(ioe);
            }
            handler.caught(channel, ioe);
        } catch (RemotingException e) {
            logger.warn(e.getMessage(), e);
        } finally {
            XsocketChannel.removeChannelIfDisconnected(connection);
        }
        return false;
    }

    @Override
    public boolean onConnect(INonBlockingConnection connection) throws BufferUnderflowException {
        XsocketChannel channel = XsocketChannel.getOrAddChannel(connection, url, handler, codec);
        try {
            if (!future.isDone()) {
                future.complete(true);
            }
            handler.connected(channel);
        } catch (RemotingException e) {
            logger.warn(e.getMessage(), e);
        } finally {
            XsocketChannel.removeChannelIfDisconnected(connection);
        }
        return false;
    }

    @Override
    public boolean onConnectionTimeout(INonBlockingConnection connection) {
        if (!future.isDone()) {
            future.complete(false);
        }
        return false;
    }

    @Override
    public boolean onData(INonBlockingConnection connection) throws BufferUnderflowException {
        try {
            XsocketChannel channel = XsocketChannel.getOrAddChannel(connection, url, handler, codec);
            channel.decodeAndDealRead(connection);
        } catch (IOException e) {
            logger.warn(e);
        } finally {
            XsocketChannel.removeChannelIfDisconnected(connection);
        }
        return false;
    }

    @Override
    public boolean onDisconnect(INonBlockingConnection connection) {
        XsocketChannel channel = XsocketChannel.getOrAddChannel(connection, this.url, this.handler, codec);
        try {
            handler.disconnected(channel);
        } catch (RemotingException e) {
            logger.warn(e);
        } finally {
            XsocketChannel.removeChannelIfDisconnected(connection);
        }
        return false;
    }



}
