package io.conio.util;

import com.offbynull.coroutines.user.Continuation;

import java.util.concurrent.ExecutionException;

/**
 * <p>
 * The future that waits in a coroutine.
 * </p>
 * @author little-pan
 * @since 2018-08-12
 */
public interface CoFuture<V> {

    V get(Continuation co)throws ExecutionException;

    boolean isDone();

}
