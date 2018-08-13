package io.conio;

import com.offbynull.coroutines.user.Continuation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * The coroutine channel handler.
 * </p>
 * @since 2018-08-09
 * @author little-pan
 */
public interface CoHandler {
    Logger log = LoggerFactory.getLogger(CoHandler.class);

    void handle(Continuation co);

    default void uncaught(Throwable cause){
        log.warn("uncaught exception", cause);
    }

}
