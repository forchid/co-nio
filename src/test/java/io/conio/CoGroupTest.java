package io.conio;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CoGroupTest {
    final static Logger log = LoggerFactory.getLogger(CoGroupTest.class);
    final static String HOST = "localhost";

    @Test
    public void testAioConnect(){
        final CoGroup serverGroup = CoGroup.newBuilder()
                .useAio(true)
                .setHost(HOST)
                .setName("serverCoGroup")
                .channelInitializer((channel, sside) -> {
                    if(sside) {
                        channel.handler(new EchoServerHandler());
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
        final int n = 10;
        final EchoClientHandler handlers[] = new EchoClientHandler[n];
        for(int i = 0; i < n; ++i){
            final EchoClientHandler handler = new EchoClientHandler(256);
            clientGroup.connect(HOST, serverGroup.getPort(), handler);
            handlers[i] = handler;
        }
        sleep(60000L);
        clientGroup.shutdown();
        clientGroup.await();
        final long sec = (System.currentTimeMillis() - ts) / 1000L;
        long bytes = 0L, times = 0L;
        for(final EchoClientHandler handler: handlers){
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

    @Test
    public void testNioConnect(){
        final CoGroup serverGroup = CoGroup.newBuilder()
                .setHost(HOST)
                .setName("serverCoGroup")
                .channelInitializer((channel, sside) -> {
                    if(sside) {
                        channel.handler(new EchoServerHandler());
                    }
                })
                .build();
        serverGroup.start();

        final CoGroup clientGroup = CoGroup.newBuilder()
                .setName("clientCoGroup")
                .build();
        clientGroup.start();

        final long ts = System.currentTimeMillis();
        final int n = 10;
        final EchoClientHandler handlers[] = new EchoClientHandler[n];
        for(int i = 0; i < n; ++i){
            final EchoClientHandler handler = new EchoClientHandler(256);
            clientGroup.connect(HOST, serverGroup.getPort(), handler);
            handlers[i] = handler;
        }
        sleep(60000L);
        clientGroup.shutdown();
        clientGroup.await();
        final long sec = (System.currentTimeMillis() - ts) / 1000L;
        long bytes = 0L, times = 0L;
        for(final EchoClientHandler handler: handlers){
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

    @Test
    public void testLongConnect(){
        final CoGroup serverGroup = CoGroup.newBuilder()
                .setHost(HOST)
                .setName("serverCoGroup")
                .channelInitializer((channel, sside) -> {
                    if(sside) {
                        channel.handler(new EchoServerHandler());
                    }
                })
                .build();
        serverGroup.start();

        final CoGroup clientGroup = CoGroup.newBuilder()
                .setName("clientCoGroup")
                .build();
        clientGroup.start();

        final long ts = System.currentTimeMillis();
        final int n = 10;
        final EchoClientHandler handlers[] = new EchoClientHandler[n];
        for(int i = 0; i < n; ++i){
            final EchoClientHandler handler = new EchoClientHandler(1024);
            clientGroup.connect(HOST, serverGroup.getPort(), handler);
            handlers[i] = handler;
        }
        sleep(300000L);
        clientGroup.shutdown();
        clientGroup.await();
        final long sec = (System.currentTimeMillis() - ts) / 1000L;
        long bytes = 0L, times = 0L;
        for(final EchoClientHandler handler: handlers){
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

    static void sleep(long millis){
        try {
            Thread.sleep(millis);
        }catch(InterruptedException e){}
    }

}
