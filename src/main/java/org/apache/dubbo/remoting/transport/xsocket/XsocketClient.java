package org.apache.dubbo.remoting.transport.xsocket;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.Version;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.remoting.Channel;
import org.apache.dubbo.remoting.ChannelHandler;
import org.apache.dubbo.remoting.Constants;
import org.apache.dubbo.remoting.RemotingException;
import org.apache.dubbo.remoting.transport.AbstractClient;
import org.xsocket.WorkerPool;
import org.xsocket.connection.IConnection;
import org.xsocket.connection.INonBlockingConnection;
import org.xsocket.connection.NonBlockingConnectionPool;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.dubbo.common.constants.CommonConstants.IO_THREADS_KEY;

public class XsocketClient extends AbstractClient {

    private static final Logger logger = LoggerFactory.getLogger(XsocketClient.class);

    private NonBlockingConnectionPool connectionPool;

    private INonBlockingConnection nbc;


    XsocketClient(URL url, ChannelHandler handler) throws RemotingException {
        super(url, wrapChannelHandler(url, handler));
    }

    @Override
    protected void doOpen() {
        connectionPool = new NonBlockingConnectionPool();
        connectionPool.setWorkerpool(new WorkerPool(getUrl().getPositiveParameter(IO_THREADS_KEY, Constants.DEFAULT_IO_THREADS), getUrl().getPositiveParameter(IO_THREADS_KEY, Constants.DEFAULT_IO_THREADS), 60, TimeUnit.SECONDS, false));
    }

    @Override
    protected void doClose() throws Throwable {
        if (nbc != null) {
            nbc.close();
        }
        if (connectionPool != null){
            connectionPool.close();
        }
    }

    @Override
    protected void doConnect() throws Throwable {
        long start = System.currentTimeMillis();
        try {
            CompletableFuture<Boolean> future = new CompletableFuture<>();
            INonBlockingConnection newConnection = connectionPool.getNonBlockingConnection(getConnectAddress().getAddress(), getConnectAddress().getPort(), new XsocketClientHander(future, getUrl(), getDelegateHandler(), getCodec()), getConnectTimeout());
            newConnection.setFlushmode(IConnection.FlushMode.ASYNC);
            INonBlockingConnection oldConnection = XsocketClient.this.nbc;
            boolean ret = future.get(getConnectTimeout(), MILLISECONDS);
            if (ret) {
                try {
                    if (oldConnection != null) {
                        try {
                            if (logger.isInfoEnabled()) {
                                logger.info("Close old xSocket channel " + oldConnection + " on create new xSocket channel " + newConnection);
                            }
                            oldConnection.close();
                        } finally {
                            XsocketChannel.removeChannelIfDisconnected(oldConnection);
                        }
                    }
                } finally {
                    if (XsocketClient.this.isClosed()) {
                        try {
                            if (logger.isInfoEnabled()) {
                                logger.info("Close new netty channel " + newConnection + ", because the client closed.");
                            }
                            newConnection.close();
                        } finally {
                            XsocketClient.this.nbc = null;
                            XsocketChannel.removeChannelIfDisconnected(newConnection);
                        }
                    } else {
                        XsocketClient.this.nbc = newConnection;
                    }
                }
            } else {
                throw new RemotingException(this, "client(url: " + getUrl() + ") failed to connect to server "
                        + getRemoteAddress() + " client-side timeout "
                        + getConnectTimeout() + "ms (elapsed: " + (System.currentTimeMillis() - start) + "ms) from xSocket client "
                        + NetUtils.getLocalHost() + " using dubbo version " + Version.getVersion());
            }
        } catch (
                ExecutionException e) {
            throw new RemotingException(this, "client(url: " + getUrl() + ") failed to connect to server "
                    + getRemoteAddress() + ", error message is:" + e.getMessage(), e.getCause());
        } catch (
                TimeoutException outTime) {
            throw new RemotingException(this, "client(url: " + getUrl() + ") failed to connect to server "
                    + getRemoteAddress() + " client-side timeout "
                    + getConnectTimeout() + "ms (elapsed: " + (System.currentTimeMillis() - start) + "ms) from xSocket client "
                    + NetUtils.getLocalHost() + " using dubbo version " + Version.getVersion());
        }

    }

    @Override
    protected void doDisConnect() {
        try {
            XsocketChannel.removeChannelIfDisconnected(nbc);
        } catch (Throwable t) {
            logger.warn(t.getMessage());
        }
    }

    @Override
    protected Channel getChannel() {
        INonBlockingConnection connection = nbc;
        if (connection == null || !connection.isOpen()) {
            return null;
        }
        return XsocketChannel.getOrAddChannel(connection, getUrl(), this, getCodec());
    }
}

