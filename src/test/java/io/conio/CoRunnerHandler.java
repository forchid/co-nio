package io.conio;

import com.offbynull.coroutines.user.Continuation;
import io.conio.util.CoFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;

public class CoRunnerHandler extends BaseTest implements CoHandler {

    final static Logger log = LoggerFactory.getLogger(CoRunnerHandler.class);

    public CoRunnerHandler(){

    }

    @Override
    public void handle(Continuation co){
        final PushCoRunner pushCo = (PushCoRunner)co.getContext();
        final CoGroup group = pushCo.group();
        for(int i = 0;!group.isShutdown(); ++i){
            final PullCoRunner pullCo = group.startCoroutine();
            final int n = i;
            final CoFuture<Long> f = pullCo.execute((c) -> {
                long sum = 0L;
                for(long j = 1; j <= n; ++j){
                    sum += j;
                }
                return sum;
            });
            try {
                final long sum = f.get(co);
                times++;
                log.debug("Sum({}): {}", n, sum);
                pushCo.yield(co);
            }catch(ExecutionException e){
                log.warn("Calc error", e);
            }finally {
                pullCo.stop();
            }
        }
    }

}
