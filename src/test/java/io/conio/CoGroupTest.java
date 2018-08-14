package io.conio;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CoGroupTest {
    final static Logger log = LoggerFactory.getLogger(CoGroupTest.class);
    final static String HOST = "localhost";

    @Test
    public void testAioConnect(){
        testConnect(true, 15000L);
    }

    @Test
    public void testNioConnect(){
        testConnect(false, 15000L);
    }

    @Test
    public void testLongConnect(){
        testConnect(false, 30000L);
    }

    private void testConnect(boolean useAio, long duration){
        final CoGroup serverGroup = CoGroup.newBuilder()
                .useAio(useAio)
                .setHost(HOST)
                .setName("serverCoGroup")
                .channelInitializer((channel, sside) -> {
                    if(sside) {
                        final PushCoChannel chan = (PushCoChannel)channel;
                        chan.handler(new EchoServerHandler());
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
        final int n = 10;
        final EchoClientHandler handlers[] = new EchoClientHandler[n];
        for(int i = 0; i < n; ++i){
            final EchoClientHandler handler = new EchoClientHandler(1024);
            clientGroup.connect(HOST, serverGroup.getPort(), handler);
            handlers[i] = handler;
        }
        BaseTest.sleep(duration);
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

}
