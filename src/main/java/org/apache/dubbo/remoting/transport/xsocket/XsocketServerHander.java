package org.apache.dubbo.remoting.transport.xsocket;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.remoting.Channel;
import org.apache.dubbo.remoting.ChannelHandler;
import org.apache.dubbo.remoting.Codec2;
import org.apache.dubbo.remoting.RemotingException;
import org.xsocket.connection.IDataHandler;
import org.xsocket.connection.IConnectHandler;
import org.xsocket.connection.IDisconnectHandler;
import org.xsocket.connection.IConnectExceptionHandler;
import org.xsocket.connection.INonBlockingConnection;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.BufferUnderflowException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class XsocketServerHander implements IDataHandler, IConnectHandler, IDisconnectHandler, IConnectExceptionHandler {
    private static final Logger logger = LoggerFactory.getLogger(XsocketServerHander.class);

    private final URL url;

    private final ChannelHandler handler;

    private final Codec2 codec;


    private final Map<String, Channel> channels = new ConcurrentHashMap<>();

    XsocketServerHander(URL url, ChannelHandler handler, Codec2 codec) {

        if (url == null) {
            throw new IllegalArgumentException("url == null");
        }
        if (handler == null) {
            throw new IllegalArgumentException("handler == null");
        }
        this.url = url;
        this.handler = handler;
        this.codec = codec;
    }

    @Override
    public boolean onConnectException(INonBlockingConnection connection, IOException ioe) {
        XsocketChannel channel = XsocketChannel.getOrAddChannel(connection, url, handler, codec);
        try {
            handler.caught(channel, ioe);
        } catch (RemotingException e) {
            logger.warn(e);
        } finally {
            XsocketChannel.removeChannelIfDisconnected(connection);
        }
        return false;
    }

    @Override
    public boolean onConnect(INonBlockingConnection connection) throws BufferUnderflowException {
        XsocketChannel channel = XsocketChannel.getOrAddChannel(connection, url, handler, codec);
        try {
            if (channel != null) {
                String channelKey = NetUtils.toAddressString(new InetSocketAddress(connection.getRemoteAddress(), connection.getRemotePort()));
                channels.put(channelKey, channel);
            }
            handler.connected(channel);
        } catch (RemotingException e) {
            logger.warn(e);
        } finally {
            XsocketChannel.removeChannelIfDisconnected(connection);
        }
        return false;
    }

    @Override
    public boolean onData(INonBlockingConnection connection) throws BufferUnderflowException {
        try {
            XsocketChannel channel = XsocketChannel.getOrAddChannel(connection, url, handler, codec);
            if (channel != null) {
                channel.decodeAndDealRead(connection);
            }

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
            String channelKey = NetUtils.toAddressString(new InetSocketAddress(connection.getRemoteAddress(), connection.getRemotePort()));
            channels.remove(channelKey);
            handler.disconnected(channel);
        } catch (RemotingException e) {
            e.printStackTrace();
        } finally {
            XsocketChannel.removeChannelIfDisconnected(connection);
        }
        return false;
    }

    Map<String, Channel> getChannels() {
        return channels;
    }


}
