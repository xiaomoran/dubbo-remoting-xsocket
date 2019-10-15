package org.apache.dubbo.remoting.transport.xsocket;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.remoting.*;

/**
 * @author lg
 */
public class XsocketTransporter implements Transporter {
    @Override
    public Server bind(URL url, ChannelHandler handler) throws RemotingException {
        return new XsocketServer(url, handler);
    }

    @Override
    public Client connect(URL url, ChannelHandler handler) throws RemotingException {
        return new XsocketClient(url, handler);
    }
}
