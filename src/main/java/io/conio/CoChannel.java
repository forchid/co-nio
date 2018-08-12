package io.conio;

import java.io.IOException;
import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import com.offbynull.coroutines.user.Continuation;
import com.offbynull.coroutines.user.Coroutine;
import com.offbynull.coroutines.user.CoroutineRunner;
import io.conio.util.CoCallable;
import io.conio.util.CoFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * The channel based on coroutine.
 * </p>
 * @since 2018-08-09
 * @author little-pan
 */
public abstract class CoChannel implements Coroutine, Closeable {
    final static Logger log = LoggerFactory.getLogger(CoChannel.class);

    public final int id;
    public final String name;

    private final CoroutineRunner runner = new CoroutineRunner(this);
    private final Queue<CoFutureTask<?>> coQueue = new LinkedList<>();
    private boolean idle;

    protected final CoGroup group;
    protected CoHandler handler;

    protected CoChannel(final int id, CoGroup group){
        this.id = id;
        this.name = "chan-co-"+id;
        this.group = group;
    }

    /**
     * <p>
     * Execute the callable in channel worker thread pool.
     * </p>
     *
     * @param callable
     * @param <V>
     * @return The callable future
     */
    public  <V> CoFuture<V> execute(final Callable<V> callable){
        return group.execute(this, callable);
    }

    /**
     * <p>
     * Execute the callable in channel coroutine.
     * </p>
     * @param coCallable
     * @param <V>
     * @return the callable future
     */
    public <V> CoFuture<V> execute(final CoCallable<V> coCallable){
        final CoFutureTask<V> f = new CoFutureTask(coCallable);
        coQueue.offer(f);
        if(isIdle()){
            resume();
        }
        return f;
    }

    @Override
    public void run(Continuation co){
        log.debug("{}: started", name);
        for(;isOpen();) {
            handleCoTasks(co);
            if(handler != null){
                handler.handle(co, this);
            }
            try{
                if(isOpen()){
                    idle = true;
                    co.suspend();
                }
            }finally {
                idle = false;
            }
        }
        coQueue.clear();
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

    public CoGroup group(){
        return group;
    }

    public CoHandler handler(){
        return handler;
    }

    public CoChannel handler(CoHandler handler){
        this.handler = handler;
        return this;
    }

    public abstract int read(Continuation co, ByteBuffer dst) throws IOException;

    public abstract int write(Continuation co, ByteBuffer src) throws IOException;

    public abstract boolean isOpen();

    public final boolean isIdle(){
        return idle;
    }

    @Override
    public abstract void close();

    final boolean resume(){
        return runner.execute();
    }

    static class CoFutureTask<V> implements CoFuture<V>{
        final CoCallable<V> coCallable;

        private boolean done;
        private V value;
        private Throwable cause;

        private CoChannel waiter;

        public CoFutureTask(final CoCallable<V> coCallable){
            this.coCallable = coCallable;
        }

        @Override
        public V get(Continuation co) throws ExecutionException {
            if(!isDone()){
                waiter = (CoChannel)co.getContext();
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
