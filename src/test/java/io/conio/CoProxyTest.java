/*
 * Copyright (c) 2018, little-pan, All rights reserved.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3.0 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library.
 */
package io.conio;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

/**
 * <p>
 * Factorial computation proxy test case.
 * </p>
 * @author little-pan
 * @since 2018-08-14
 */
public class CoProxyTest {
    final static Logger log = LoggerFactory.getLogger(CoProxyTest.class);

    final static String PROXY_HOST = "localhost";
    final static int PROXY_PORT    = 9690;

    final static String BACKEND_HOSTS[] = {"localhost", "localhost"};
    final static int BACKEND_PORTS[]    = {9091, 9092};

    @Test
    public void testProxy(){
        log.info("Start factorial backend servers");
        final CoGroup backendGroups[] = new CoGroup[BACKEND_HOSTS.length];
        final InetSocketAddress backends[] = new InetSocketAddress[BACKEND_HOSTS.length];
        for(int i = 0, size = BACKEND_HOSTS.length; i < size; ++i){
            final String host = BACKEND_HOSTS[i];
            final int port = BACKEND_PORTS[i];
            final InetSocketAddress backend = new InetSocketAddress(host, port);
            backends[i] = backend;
            final CoGroup group = CoGroup.newBuilder()
                    .setHost(host)
                    .setPort(port)
                    .setName("backendGroup"+i)
                    .channelInitializer((channel, sside) -> {
                        if(sside) {
                            final PushCoChannel chan = (PushCoChannel)channel;
                            chan.handler(new FactorialServerHandler());
                        }
                    })
                    .build();
            group.start();
            backendGroups[i] = group;
        }

        log.info("Start factorial proxy server");
        final PullChannelPool.HeartbeatCodec heartbeatCodec = new HeartbeatClientCodec();
        final CoGroup proxyGroup = CoGroup.newBuilder()
                .setHost(PROXY_HOST)
                .setPort(PROXY_PORT)
                .setName("proxyGroup")
                .setPullChannelPoolHeartbeatInterval(1)
                .setPullChannelPoolHeartbeatCodec(heartbeatCodec)
                .channelInitializer((channel, sside) -> {
                    if(sside) {
                        final PushCoChannel chan = (PushCoChannel)channel;
                        chan.handler(new FactorialProxyHandler(backends));
                    }
                })
                .build();
        proxyGroup.start();

        log.info("Start factorial client");
        final CoGroup clientGroup =  CoGroup.newBuilder()
                .setName("clientGroup")
                .build();
        clientGroup.start();
        final long ts = System.currentTimeMillis();
        final int clients = 128;
        final FactorialClientHandler clientHandlers[] = new FactorialClientHandler[clients];
        for(int i = 0; i < clients; ++i){
            final FactorialClientHandler handler = new FactorialClientHandler();
            clientGroup.connect(PROXY_HOST, PROXY_PORT, handler);
            clientHandlers[i] = handler;
        }

        log.info("Test bootstrap");
        BaseTest.sleep(15000L);
        final long sec = (System.currentTimeMillis() - ts) / 1000L;
        long bytes = 0L, times = 0L;
        for(final FactorialClientHandler handler: clientHandlers){
            bytes += handler.bytes;
            times += handler.times;
        }
        if(sec == 0L){
            log.info("bytes: {}m, times: {}", bytes>>20, times);
        }else{
            log.info("bytes: {}m, tps: {}", bytes>>20, times/sec);
        }

        log.info("Test shutdown");
        clientGroup.shutdown();
        clientGroup.await();

        if(heartbeatCodec != null){
            log.info("Test heartbeat");
            BaseTest.sleep(15000L);
        }

        proxyGroup.shutdown();
        proxyGroup.await();

        for(final CoGroup group: backendGroups){
            group.shutdown();
            group.await();
        }

    }

    public static void main(String args[]){
        final CoProxyTest test = new CoProxyTest();
        test.testProxy();
    }

}
