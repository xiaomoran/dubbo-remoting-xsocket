package org.apache.dubbo.remoting.transport.xsocket;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.remoting.ChannelHandler;
import org.apache.dubbo.remoting.Codec2;
import org.apache.dubbo.remoting.RemotingException;
import org.apache.dubbo.remoting.buffer.ChannelBuffer;
import org.apache.dubbo.remoting.buffer.ChannelBuffers;
import org.apache.dubbo.remoting.buffer.DynamicChannelBuffer;
import org.apache.dubbo.remoting.transport.AbstractChannel;
import org.xsocket.connection.IConnection;
import org.xsocket.connection.INonBlockingConnection;
import org.xsocket.connection.IWriteCompletionHandler;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.*;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.dubbo.common.constants.CommonConstants.DEFAULT_TIMEOUT;
import static org.apache.dubbo.common.constants.CommonConstants.TIMEOUT_KEY;
import static org.apache.dubbo.remoting.Constants.*;


public class XsocketChannel extends AbstractChannel {

    private static final Logger logger = LoggerFactory.getLogger(XsocketChannel.class);

    private ChannelBuffer readBuffer = ChannelBuffers.EMPTY_BUFFER;


    private final Map<String, Object> attributes = new ConcurrentHashMap<>();

    private static final ConcurrentMap<INonBlockingConnection, XsocketChannel> CONNECTION_MAP = new ConcurrentHashMap<>();

    private final INonBlockingConnection connection;

    private final Codec2 codec;

    private final int bufferSize;

    private XsocketChannel(URL url, ChannelHandler handler, INonBlockingConnection connection, Codec2 codec) {
        super(url, handler);
        int b = url.getPositiveParameter(BUFFER_KEY, DEFAULT_BUFFER_SIZE);
        this.bufferSize = b >= MIN_BUFFER_SIZE && b <= MAX_BUFFER_SIZE ? b : DEFAULT_BUFFER_SIZE;
        this.connection = connection;
        this.codec = codec;
    }

    @Override
    public InetSocketAddress getRemoteAddress() {
        return new InetSocketAddress(connection.getRemoteAddress(), connection.getRemotePort());
    }

    @Override
    public boolean isConnected() {
        return !isClosed() && connection.isOpen();
    }

    @Override
    public boolean hasAttribute(String key) {
        return attributes.containsKey(key);
    }

    @Override
    public Object getAttribute(String key) {
        return attributes.get(key);
    }

    @Override
    public void setAttribute(String key, Object value) {
        if (value == null) {
            attributes.remove(key);
        } else {
            attributes.put(key, value);
        }
    }

    @Override
    public void removeAttribute(String key) {
        attributes.remove(key);
    }

    @Override
    public InetSocketAddress getLocalAddress() {
        return new InetSocketAddress(connection.getLocalAddress(), connection.getLocalPort());
    }

    @Override
    public void send(Object message, boolean sent) throws RemotingException {
        encodeAndDealWrite(message, sent);
    }


    private void encodeAndDealWrite(Object message, boolean sent) throws RemotingException {
        super.send(message, sent);
        int timeout = 0;
        try {
            if (connection.isOpen()) {
                if (sent) {
                    CompletableFuture<Boolean> future = sendMsgFuture(message);
                    timeout = getUrl().getPositiveParameter(TIMEOUT_KEY, DEFAULT_TIMEOUT);
                    future.get(timeout, MILLISECONDS);
                } else {
                    connection.setFlushmode(IConnection.FlushMode.ASYNC);
                    connection.write(bulidBuffer(message));
                    connection.flush();
                }
            }
        } catch (ExecutionException | IOException | InterruptedException e) {
            throw new RemotingException(this, "Failed to send message " + message + " to " + getRemoteAddress() + ", cause: " + e.getMessage(), e);
        } catch (TimeoutException e) {
            throw new RemotingException(this, "Failed to send message " + message + " to " + getRemoteAddress() + "in timeout(" + timeout + "ms) limit");
        }
    }


    private ByteBuffer bulidBuffer(Object msg) throws IOException {
        ChannelBuffer witeBuffer = ChannelBuffers.dynamicBuffer(1024);
        codec.encode(this, witeBuffer, msg);
        return witeBuffer.toByteBuffer();
    }

    private CompletableFuture<Boolean> sendMsgFuture(Object msg) throws IOException {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        connection.write(bulidBuffer(msg), new IWriteCompletionHandler() {
            @Override
            public void onWritten(int written) {
                if (!future.isDone()) {
                    future.complete(true);
                }
            }

            @Override
            public void onException(IOException ioe) {
                future.completeExceptionally(ioe);
            }
        });
        connection.setFlushmode(IConnection.FlushMode.ASYNC);
        connection.flush();
        return future;
    }

    void decodeAndDealRead(INonBlockingConnection connection) throws IOException {
        int readable = connection.available();
        if (readable <= 0) {
            return;
        }
        byte[] bytes;
        ChannelBuffer frame;
        do {
            readable = connection.available();
            readable = readable > bufferSize ? bufferSize : readable;
            bytes = connection.readBytesByLength(readable);
            if (readBuffer.readable()) {
                if (readBuffer instanceof DynamicChannelBuffer) {
                    readBuffer.writeBytes(bytes);
                    frame = readBuffer;
                } else {
                    int size = readBuffer.readableBytes() + readable;
                    frame = ChannelBuffers.dynamicBuffer(size > bufferSize ? size : bufferSize);
                    frame.writeBytes(readBuffer, readBuffer.readableBytes());
                    frame.writeBytes(bytes);
                }
            } else {
                frame = ChannelBuffers.wrappedBuffer(bytes);
            }
            Object msg;
            int savedReadIndex;

            try {
                do {
                    savedReadIndex = frame.readerIndex();
                    try {
                        msg = codec.decode(this, frame);
                    } catch (Exception e) {
                        readBuffer = ChannelBuffers.EMPTY_BUFFER;
                        throw e;
                    }
                    if (msg == Codec2.DecodeResult.NEED_MORE_INPUT) {
                        frame.readerIndex(savedReadIndex);
                        break;
                    } else {
                        if (savedReadIndex == frame.readerIndex()) {
                            readBuffer = ChannelBuffers.EMPTY_BUFFER;
                            throw new Exception("Decode without read data.");
                        }
                        if (msg != null) {
                            this.received(this, msg);
                        }
                    }


                } while (frame.readable());
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (frame.readable()) {
                    frame.discardReadBytes();
                    readBuffer = frame;
                } else {
                    readBuffer = ChannelBuffers.EMPTY_BUFFER;
                }
                XsocketChannel.removeChannelIfDisconnected(connection);
            }
        } while (readable > bufferSize);

    }

    @Override
    public void close() {
        try {
            super.close();
        } catch (Exception e) {
            logger.warn(e.getMessage(), e);
        }
        try {
            removeChannelIfDisconnected(connection);
        } catch (Exception e) {
            logger.warn(e.getMessage(), e);
        }
        try {
            attributes.clear();
        } catch (Exception e) {
            logger.warn(e.getMessage(), e);
        }
        try {
            if (logger.isInfoEnabled()) {
                logger.info("Close xSocket channel " + connection);
            }
            connection.close();
        } catch (Exception e) {
            logger.warn(e.getMessage(), e);
        }
    }

    static void removeChannelIfDisconnected(INonBlockingConnection connection) {
        if (connection != null && !connection.isOpen()) {
            CONNECTION_MAP.remove(connection);
        }
    }


    static XsocketChannel getOrAddChannel(INonBlockingConnection connection, URL url, ChannelHandler handler, Codec2 codec) {
        if (connection == null) {
            return null;
        }
        XsocketChannel ret = CONNECTION_MAP.get(connection);
        if (ret == null) {
            XsocketChannel nettyChannel = new XsocketChannel(url, handler, connection, codec);
            if (connection.isOpen()) {
                ret = CONNECTION_MAP.putIfAbsent(connection, nettyChannel);
            }
            if (ret == null) {
                ret = nettyChannel;
            }
        }
        return ret;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((connection == null) ? 0 : connection.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        XsocketChannel other = (XsocketChannel) obj;
        if (connection == null) {
            return other.connection == null;
        } else {
            return connection.equals(other.connection);
        }
    }

    @Override
    public String toString() {
        return "XsocketChannel [INonBlockingConnection=" + connection + "]";
    }

}
