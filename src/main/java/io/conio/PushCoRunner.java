package io.conio;

import com.offbynull.coroutines.user.Continuation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * The push model CoRunner that pushes the processing logic to the handler.
 * </p>
 * @autor little-pan
 * @since 2018-08-18
 */
public class PushCoRunner extends CoRunner {
    final static Logger log = LoggerFactory.getLogger(PushCoRunner.class);

    protected CoHandler handler;

    public PushCoRunner(int id, CoGroup group){
        this(id, "push-co-"+id, group);
    }

    public PushCoRunner(int id, String name, CoGroup group){
        super(id, name, group);
    }

    @Override
    public void run(Continuation co){
        log.debug("{}: Started", name);
        co.setContext(this);
        handler.handle(co);
        log.debug("{}: Stopped", name);
    }

    public CoHandler handler(){
        return handler;
    }

    public PushCoRunner handler(CoHandler handler){
        this.handler = handler;
        return this;
    }

}
