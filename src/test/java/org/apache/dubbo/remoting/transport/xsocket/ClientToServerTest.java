/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dubbo.remoting.transport.xsocket;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.remoting.RemotingException;
import org.apache.dubbo.remoting.exchange.ExchangeChannel;
import org.apache.dubbo.remoting.exchange.ExchangeServer;
import org.apache.dubbo.remoting.exchange.Exchangers;
import org.apache.dubbo.remoting.exchange.support.Replier;
import org.apache.dubbo.rpc.Protocol;
import org.apache.dubbo.rpc.ProxyFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


import java.io.IOException;
import java.rmi.RemoteException;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.fail;

/**
 * ClientToServer
 */
public class ClientToServerTest {
    private Protocol protocol = ExtensionLoader.getExtensionLoader(Protocol.class).getAdaptiveExtension();
    private ProxyFactory proxy = ExtensionLoader.getExtensionLoader(ProxyFactory.class).getAdaptiveExtension();

    private ExchangeServer server;

    private ExchangeChannel client;

    private WorldHandler handler = new WorldHandler();

    ExchangeServer newServer(int port, Replier<?> receiver) throws RemotingException {
        return Exchangers.bind(URL.valueOf("exchange://localhost:" + port + "?server=xSocket"), receiver);
    }

    ExchangeChannel newClient(int port) throws RemotingException {
        return Exchangers.connect(URL.valueOf("exchange://localhost:" + port + "?client=xSocket&timeout=3000"));
    }

    @BeforeEach
    protected void setUp() throws Exception {
        int port = NetUtils.getAvailablePort();
        server = newServer(port, handler);
        client = newClient(port);
    }

    @AfterEach
    protected void tearDown() {
        try {
            if (server != null)
                server.close();
        } finally {
            if (client != null)
                client.close();
        }
    }

    @Test
    public void testFuture() throws Exception {
        CompletableFuture<Object> future = client.request(new World("world"));
        Hello result = (Hello) future.get();
        Assertions.assertEquals("hello,world", result.getName());
    }


    @Test
    public void testRpc() throws Exception {
        RemoteService remoteService = new RemoteServiceImpl();
        URL url = URL.valueOf("dubbo://127.0.0.1:9012/org.apache.dubbo.remoting.transport.xsocket.RemoteService?service.filter=echo&timeout=3000&server=xSocket&connect.timeout=3000&heartbeat=3000&client=xSocket");
        protocol.export(proxy.getInvoker(remoteService, RemoteService.class, url));
        RemoteService services = proxy.getProxy(protocol.refer(RemoteService.class, url));
        Assertions.assertEquals(services.sayHello("world"),"hello#world");



    }

    @Test
    public void testMultiThread() throws Exception {
        int tc = 2000;
        ExecutorService exec = Executors.newFixedThreadPool(20);
        final CountDownLatch latch = new CountDownLatch(tc);
        for (int i = 0; i < tc; i++) {
            int finalI = i;
            exec.execute(() -> {
                CompletableFuture<Object> future = null;
                try {
                    future = client.request(new World("world" + finalI));
                    Hello result = (Hello) future.get();
                    Assertions.assertEquals("hello,world" + finalI, result.getName());
                } catch (RemotingException | InterruptedException | ExecutionException e) {
                    fail();
                }

                latch.countDown();
            });
        }
        ;
        latch.await();
        exec.shutdown();
        exec.awaitTermination(10, TimeUnit.SECONDS);
    }



}