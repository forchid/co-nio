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

public class TaskExecutionTest {
    final static Logger log = LoggerFactory.getLogger(TaskExecutionTest.class);

    final static String HOST = "localhost";
    final static int maxConns = 128;

    @Test
    public void testNioTaskExecution(){
        testTaskExecution(false, 15000L);
    }

    @Test
    public void testAioTaskExecution(){
        testTaskExecution(true, 15000L);
    }

    private void testTaskExecution(boolean useAio, long duration){
        final CoGroup serverGroup = CoGroup.newBuilder()
                .useAio(useAio)
                .setHost(HOST)
                .setName("serverCoGroup")
                .channelInitializer((channel, sside) -> {
                    if(sside) {
                        final PushCoChannel chan = (PushCoChannel)channel;
                        chan.handler(new FactorialServerHandler());
                    }
                })
                .build();
        serverGroup.start();

        final CoGroup clientGroup = CoGroup.newBuilder()
                .useAio(useAio)
                .setName("clientCoGroup")
                .build();
        clientGroup.start();

        final long ts = System.currentTimeMillis();
        final int n = maxConns;
        final FactorialClientHandler handlers[] = new FactorialClientHandler[n];
        for(int i = 0; i < n; ++i){
            final FactorialClientHandler handler = new FactorialClientHandler();
            clientGroup.connect(HOST, serverGroup.getPort(), handler);
            handlers[i] = handler;
        }

        BaseTest.sleep(duration);

        clientGroup.shutdown();
        clientGroup.await();
        final long sec = (System.currentTimeMillis() - ts) / 1000L;
        long bytes = 0L, times = 0L;
        for(final FactorialClientHandler handler: handlers){
            bytes += handler.bytes;
            times += handler.times;
        }
        if(sec == 0L){
            log.info("bytes: {}m, times: {}", bytes>>20, times);
        }else{
            log.info("bytes: {}m, tps: {}", bytes>>20, times/sec);
        }

        serverGroup.shutdown();
        serverGroup.await();
    }

    public static void main(String args[]){
        final TaskExecutionTest test = new TaskExecutionTest();
        test.testNioTaskExecution();
        test.testAioTaskExecution();
    }

}
