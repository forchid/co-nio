package io.conio;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TaskExecutionTest {
    final static Logger log = LoggerFactory.getLogger(TaskExecutionTest.class);
    final static String HOST = "localhost";

    @Test
    public void testAioTaskExecution(){
        final CoGroup serverGroup = CoGroup.newBuilder()
                .useAio(true)
                .setHost(HOST)
                .setName("serverCoGroup")
                .channelInitializer((channel, sside) -> {
                    if(sside) {
                        channel.handler(new FactorialServerHandler());
                    }
                })
                .build();
        serverGroup.start();

        final CoGroup clientGroup = CoGroup.newBuilder()
                .useAio(true)
                .setName("clientCoGroup")
                .build();
        clientGroup.start();

        final long ts = System.currentTimeMillis();
        final int n = 200;
        final FactorialClientHandler handlers[] = new FactorialClientHandler[n];
        for(int i = 0; i < n; ++i){
            final FactorialClientHandler handler = new FactorialClientHandler();
            clientGroup.connect(HOST, serverGroup.getPort(), handler);
            handlers[i] = handler;
        }
        sleep(15000L);
        clientGroup.shutdown();
        clientGroup.await();
        final long sec = (System.currentTimeMillis() - ts) / 1000L;
        long bytes = 0L, times = 0L, maxlen = 0;
        for(final FactorialClientHandler handler: handlers){
            bytes += handler.bytes;
            times += handler.times;
            if(maxlen < handler.maxlen){
                maxlen = handler.maxlen;
            }
        }
        if(sec == 0L){
            log.info("bytes: {}m, times: {}, maxlen: {}", bytes>>20, times, maxlen);
        }else{
            log.info("bytes: {}m, tps: {}, maxlen: {}", bytes>>20, times/sec, maxlen);
        }

        serverGroup.shutdown();
        serverGroup.await();
    }

    @Test
    public void testNioTaskExecution(){
        final CoGroup serverGroup = CoGroup.newBuilder()
                .setHost(HOST)
                .setName("serverCoGroup")
                .channelInitializer((channel, sside) -> {
                    if(sside) {
                        channel.handler(new FactorialServerHandler());
                    }
                })
                .build();
        serverGroup.start();

        final CoGroup clientGroup = CoGroup.newBuilder()
                .setName("clientCoGroup")
                .build();
        clientGroup.start();

        final long ts = System.currentTimeMillis();
        final int n = 200;
        final FactorialClientHandler handlers[] = new FactorialClientHandler[n];
        for(int i = 0; i < n; ++i){
            final FactorialClientHandler handler = new FactorialClientHandler();
            clientGroup.connect(HOST, serverGroup.getPort(), handler);
            handlers[i] = handler;
        }
        sleep(15000L);
        clientGroup.shutdown();
        clientGroup.await();
        final long sec = (System.currentTimeMillis() - ts) / 1000L;
        long bytes = 0L, times = 0L, maxlen = 0;
        for(final FactorialClientHandler handler: handlers){
            bytes += handler.bytes;
            times += handler.times;
            if(maxlen < handler.maxlen){
                maxlen = handler.maxlen;
            }
        }
        if(sec == 0L){
            log.info("bytes: {}m, times: {}, maxlen: {}", bytes>>20, times, maxlen);
        }else{
            log.info("bytes: {}m, tps: {}, maxlen: {}", bytes>>20, times/sec, maxlen);
        }

        serverGroup.shutdown();
        serverGroup.await();
    }

    static void sleep(long millis){
        try {
            Thread.sleep(millis);
        }catch(InterruptedException e){}
    }

}
