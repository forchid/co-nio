package io.conio;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TaskExecutionTest {
    final static Logger log = LoggerFactory.getLogger(TaskExecutionTest.class);

    final static String HOST = "localhost";
    final static int maxConns = 100;

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

    public static void main(String args[]){
        final TaskExecutionTest test = new TaskExecutionTest();
        test.testNioTaskExecution();
        test.testAioTaskExecution();
    }

}
