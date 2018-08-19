package io.conio.util;

/**
 * <p>
 *     The coroutine future listener.
 * </p>
 * @author little-pan
 * @since 2018-08-19
 */
public interface CoFutureListener<V> {

    void operationComplete(V value, Throwable cause);

}
