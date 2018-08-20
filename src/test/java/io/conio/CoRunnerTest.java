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

public class CoRunnerTest {
    final static Logger log = LoggerFactory.getLogger(CoRunnerTest.class);

    @Test
    public void testNioCoRunner(){
        testCoRunner(false);
    }

    @Test
    public void testAioCoRunner(){
        testCoRunner(true);
    }

    private void testCoRunner(boolean aio){
        final CoGroup group = CoGroup.newBuilder()
                .useAio(aio).build();
        group.start();

        final long ts = System.currentTimeMillis();
        final int n = 128, duration = 15000;
        final CoRunnerHandler handlers[] = new CoRunnerHandler[n];
        for(int i = 0; i < n; ++i){
            final CoRunnerHandler handler = new CoRunnerHandler();
            group.startCoroutine(handler);
            handlers[i] = handler;
        }

        BaseTest.sleep(duration);

        group.shutdown();
        group.await();
        final long sec = (System.currentTimeMillis() - ts) / 1000L;
        long times = 0L;
        for(final CoRunnerHandler handler: handlers){
            times += handler.times;
        }
        if(sec == 0L){
            log.info("times: {}", times);
        }else{
            log.info("tps: {}", times/sec);
        }
    }

    public static void main(String args[]){
        final CoRunnerTest test = new CoRunnerTest();
        test.testNioCoRunner();
        test.testAioCoRunner();
    }

}
