package io.conio;

import com.offbynull.coroutines.user.Continuation;
import io.conio.util.CoFuture;
import io.conio.util.ScheduledCoFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;

public class ScheduledHandler extends BaseTest implements CoHandler {

    final LoggerHandler handler;

    public ScheduledHandler(String name, long delay){
        this.handler = new LoggerHandler(name, delay, -1L, 1);
    }

    public ScheduledHandler(String name, long delay, long period, int maxTimes){
        this.handler = new LoggerHandler(name, delay, period, maxTimes);
    }

    @Override
    public void handle(Continuation co){
        final CoRunner coRun = (CoRunner)co.getContext();
        final ScheduledCoFuture<?> future;
        if(handler.period < 1){
            future = coRun.schedule(handler, handler.delay);
        }else {
            future = coRun.schedule(handler, handler.delay, handler.period);
        }
        handler.future = future;
    }

    static class LoggerHandler implements CoHandler {
        final static Logger log = LoggerFactory.getLogger(LoggerHandler.class);

        public final String name;
        public final long delay, period;
        public final int maxTimes;
        private int times;

        public ScheduledCoFuture<?> future;

        public LoggerHandler(String name, long delay, long period, int maxTimes){
            this.name  = name;
            this.delay = delay;
            this.period= period;
            this.maxTimes = maxTimes;
        }

        @Override
        public void handle(Continuation co) {
            if(++times > maxTimes){
                log.info("{}: cancel at time-{}", name, times);
                future.cancel(true);
                return;
            }
            log.info("{}: time-{}, running", name, times);
        }
    }

}
