package io.conio;

import java.io.IOException;
import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.concurrent.Callable;

import com.offbynull.coroutines.user.Continuation;
import com.offbynull.coroutines.user.Coroutine;
import com.offbynull.coroutines.user.CoroutineRunner;
import io.conio.util.CoFuture;
import io.conio.util.ScheduledCoFuture;

/**
 * <p>
 * The channel based on coroutine.
 * </p>
 * @since 2018-08-09
 * @author little-pan
 */
public interface CoChannel extends Closeable {

    int id();
    String name();

    CoGroup group();

    <V> CoFuture<V> execute(final Callable<V> callable);

    ScheduledCoFuture<?> schedule(CoHandler handler, final long delay);
    ScheduledCoFuture<?> schedule(CoHandler handler, long initialDelay, long period);

    int read(Continuation co, ByteBuffer dst) throws IOException;
    int write(Continuation co, ByteBuffer src) throws IOException;

    boolean isOpen();

    @Override
    void close();

}
