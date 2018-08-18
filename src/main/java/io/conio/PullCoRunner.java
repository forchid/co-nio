package io.conio;

import com.offbynull.coroutines.user.Continuation;
import io.conio.util.CoCallable;
import io.conio.util.CoFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ExecutionException;

/**
 * <p>
 * The pull mode CoRunner.
 * </p>
 * @since 2018-08-18
 * @author little-pan
 */
public class PullCoRunner extends CoRunner {
    final static Logger log = LoggerFactory.getLogger(PullCoRunner.class);

    private final Queue<CoFutureTask<?>> coQueue = new LinkedList<>();
    private boolean idle;
    private boolean stopped;

    public PullCoRunner(final int id, CoGroup group){
        super(id, "pull-co-"+id, group);
    }

    public PullCoRunner(final int id, String name, CoGroup group){
        super(id, name, group);
    }

    /**
     * <p>
     * Execute the callable in the coroutine.
     * </p>
     * @param coCallable
     * @param <V>
     * @return the callable future
     */
    public <V> CoFuture<V> execute(final CoCallable<V> coCallable){
        final CoFutureTask<V> f = new CoFutureTask<>(coCallable);
        coQueue.offer(f);
        if(isIdle()){
            resume();
        }
        return f;
    }

    public boolean isStopped(){
        return stopped;
    }

    public void stop(){
        if(isStopped()){
            return;
        }
        stopped = true;
        resume();
    }

    @Override
    public void run(Continuation co){
        co.setContext(this);
        log.debug("{}: Started", name);

        for(;!isStopped();) {
            handleCoTasks(co);
            try{
                if(!isStopped()){
                    idle = true;
                    co.suspend();
                }
            }finally {
                idle = false;
            }
        }
        coQueue.clear();
        stopped = true;
        log.debug("{}: Stopped", name);
    }

    private void handleCoTasks(Continuation co){
        for(;;){
            final CoFutureTask<?> f = coQueue.poll();
            if(f == null){
                break;
            }
            try {
                final CoCallable<?> callable = f.coCallable;
                final Object result = callable.call(co);
                f.setValue(result);
            }catch(final Throwable cause){
                f.setCause(cause);
            }
        }
    }

    public final boolean isIdle(){
        return idle;
    }

    static class CoFutureTask<V> implements CoFuture<V>{
        final CoCallable<V> coCallable;

        private boolean done;
        private V value;
        private Throwable cause;

        private CoRunner waiter;

        public CoFutureTask(final CoCallable<V> coCallable){
            this.coCallable = coCallable;
        }

        @Override
        public V get(Continuation co) throws ExecutionException {
            if(!isDone()){
                waiter = (CoRunner)co.getContext();
                co.suspend();
            }
            if(cause != null){
                if(cause instanceof  ExecutionException){
                    throw (ExecutionException)cause;
                }
                throw new ExecutionException(cause);
            }
            return value;
        }

        @Override
        public boolean isDone() {
            return done;
        }

        @SuppressWarnings("unchecked")
        void setValue(Object value){
            this.value = (V)value;
            setDone();
        }

        void setCause(Throwable cause){
            this.cause = cause;
            setDone();
        }

        private void setDone(){
            this.done = true;
            if(waiter != null){
                waiter.resume();
                waiter = null;
            }
        }

    }// CoFutureTask

}
