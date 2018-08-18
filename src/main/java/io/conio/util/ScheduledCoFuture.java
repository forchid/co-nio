package io.conio.util;

import com.offbynull.coroutines.user.Continuation;

import java.util.concurrent.ExecutionException;

/**
 * <p>
 * The scheduled future that waits in a coroutine.
 * </p>
 * @author little-pan
 * @since 2018-08-18
 */
public interface ScheduledCoFuture<V> extends CoFuture<V> {

    boolean isCancelled();
    boolean cancel(boolean mayInterruptIfRunning);

    long getDelay();

}
