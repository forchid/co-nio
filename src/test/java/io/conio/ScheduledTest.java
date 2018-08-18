package io.conio;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScheduledTest {
    final static Logger log = LoggerFactory.getLogger(ScheduledTest.class);

    @Test
    public void testNioSchedule(){
        testSchedule(false);
    }

    @Test
    public void testAioSchedule(){
        testSchedule(true);
    }

    private void testSchedule(boolean aio){
        final CoGroup group = CoGroup.newBuilder()
                .useAio(aio).build();
        group.start();

        final int n = 10, duration = 15000;
        final ScheduledHandler handlers[] = new ScheduledHandler[n];
        for(int i = 0; i < n; ++i){
            final String name = "ScheduledHandler-"+i;
            final ScheduledHandler handler;
            if((i % 2) == 0){
                handler = new ScheduledHandler(name, i * 1000L);
            }else{
                handler = new ScheduledHandler(name, i * 1000L, (i+1) * 1000L, i + 1);
            }
            group.startCoroutine(handler);
            handlers[i] = handler;
        }

        BaseTest.sleep(duration);

        group.shutdown();
        group.await();
    }

    public static void main(String args[]){
        final ScheduledTest test = new ScheduledTest();
        test.testNioSchedule();
        test.testAioSchedule();
    }

}
