package io.conio;

import java.io.IOException;
import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.concurrent.Callable;

import com.offbynull.coroutines.user.Continuation;
import com.offbynull.coroutines.user.Coroutine;
import com.offbynull.coroutines.user.CoroutineRunner;
import io.conio.util.CoFuture;

/**
 * <p>
 * The channel based on coroutine.
 * </p>
 * @since 2018-08-09
 * @author little-pan
 */
public abstract class CoChannel implements Coroutine, Closeable {
    public final int id;
    public final String name;

    protected final CoGroup group;
    private final CoroutineRunner runner = new CoroutineRunner(this);

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

    @Override
    public void run(Continuation co){
        // default-noop
    }

    public CoGroup group(){
        return group;
    }

    public abstract int read(Continuation co, ByteBuffer dst) throws IOException;

    public abstract int write(Continuation co, ByteBuffer src) throws IOException;

    public abstract boolean isOpen();

    @Override
    public abstract void close();

    final boolean resume(){
        return runner.execute();
    }

}
