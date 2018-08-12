package io.conio.util;

import com.offbynull.coroutines.user.Continuation;

/**
 * <p>
 * A callable that runs on a coroutine.
 * </p>
 * @param <V>
 * @author little-pan
 * @since 2018-08-12
 */
public interface CoCallable<V> {

    V call(Continuation co);

}
