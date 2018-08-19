package io.conio;

import com.offbynull.coroutines.user.Continuation;
import com.offbynull.coroutines.user.Coroutine;
import com.offbynull.coroutines.user.CoroutineRunner;
import io.conio.util.CoFuture;
import io.conio.util.ScheduledCoFuture;

import java.util.concurrent.Callable;

/**
 * <p>
 * The coroutine runner.
 * </p>
 * @autor little-pan
 * @since 2018-08-18
 */
public abstract class CoRunner implements Coroutine {
    final CoRunner wrapped;

    final CoroutineRunner runner;
    protected final CoGroup group;

    public final int id;
    public final String name;

    protected CoRunner(int id, String name, CoGroup group){
        this.id = id;
        this.name = name;
        this.group = group;
        this.runner = new CoroutineRunner(this);
        this.wrapped = null;
    }

    protected CoRunner(CoRunner wrapped){
        this.id = wrapped.id;
        this.name = wrapped.name;
        this.group = wrapped.group;
        this.runner = wrapped.runner;
        this.wrapped = wrapped;
    }

    public int id(){
        return id;
    }

    public String name(){
        return name;
    }

    @Override
    public abstract void run(Continuation co);

    public void yield(Continuation co){
        group.yield(co);
    }

    /**
     * <p>
     *     Execute the callable in channel worker thread pool.
     * </p>
     *
     * @param callable
     * @param <V>
     * @return The callable future
     */
    public <V> CoFuture<V> execute(final Callable<V> callable){
        return group.execute(this, callable);
    }

    /**
     * <p>
     *     Schedule handler after delay millis.
     * </p>
     * @param handler
     * @param delay
     * @author little-pan
     * @since 2018-08-18
     *
     * @return a scheduled coroutine future
     */
    public ScheduledCoFuture<?> schedule(CoHandler handler, final long delay){
        return group.schedule(handler, delay);
    }

    /**
     * <p>
     *     Schedule the handler after init delay millis, then schedule it at fixed period.
     * </p>
     * @param handler
     * @param initialDelay
     * @param period
     * @author little-pan
     * @since 2018-08-18
     *
     * @return the scheduled coroutine future
     */
    public ScheduledCoFuture<?> schedule(CoHandler handler, long initialDelay, long period){
        return group.schedule(handler, initialDelay, period);
    }

    public CoGroup group(){
        return group;
    }

    final boolean resume(){
        return runner.execute();
    }

    @Override
    public String toString(){
        return name;
    }

}
