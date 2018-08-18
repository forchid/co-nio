package io.conio;

import com.offbynull.coroutines.user.Continuation;
import com.offbynull.coroutines.user.Coroutine;
import com.offbynull.coroutines.user.CoroutineRunner;
import io.conio.util.CoFuture;

import java.util.concurrent.Callable;

/**
 * <p>
 * The coroutine runner.
 * </p>
 * @autor little-pan
 * @since 2018-08-18
 */
public abstract class CoRunner implements Coroutine {

    final CoroutineRunner runner = new CoroutineRunner(this);
    protected final CoGroup group;

    public final int id;
    public final String name;

    protected CoRunner(int id, String name, CoGroup group){
        this.id = id;
        this.name = name;
        this.group = group;
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
     * Execute the callable in channel worker thread pool.
     * </p>
     *
     * @param callable
     * @param <V>
     * @return The callable future
     */
    public <V> CoFuture<V> execute(final Callable<V> callable){
        return group.execute(this, callable);
    }

    public CoGroup group(){
        return group;
    }

    final boolean resume(){
        return runner.execute();
    }

}
