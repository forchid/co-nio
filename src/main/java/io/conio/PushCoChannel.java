package io.conio;

import com.offbynull.coroutines.user.Continuation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * The push model CoChannel that pushes the processing logic to the handler.
 * </p>
 * @since 2018-08-09
 * @author little-pan
 */
public abstract class PushCoChannel extends CoChannel {
    final static Logger log = LoggerFactory.getLogger(PushCoChannel.class);

    protected CoHandler handler;

    protected PushCoChannel(final int id, CoGroup group){
        super(id, group);
    }

    @Override
    public void run(Continuation co){
        log.debug("{}: started", name);
        co.setContext(this);
        handler.handle(co);
    }

    public CoHandler handler(){
        return handler;
    }

    public PushCoChannel handler(CoHandler handler){
        this.handler = handler;
        return this;
    }

}
