package io.conio;

import java.io.IOException;
import java.io.Closeable;
import java.nio.ByteBuffer;

import com.offbynull.coroutines.user.Continuation;
import com.offbynull.coroutines.user.Coroutine;
import com.offbynull.coroutines.user.CoroutineRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * The channel based on coroutine.
 * </p>
 * @since 2018-08-09
 * @author little-pan
 */
public abstract class CoChannel implements Coroutine, Closeable {
    final static Logger log = LoggerFactory.getLogger(CoChannel.class);

    public final int id;
    public final String name;

    final CoroutineRunner runner = new CoroutineRunner(this);

    protected final CoGroup group;
    protected CoHandler handler;

    protected CoChannel(final int id, CoGroup group){
        this.id = id;
        this.name = "chan-co-"+id;
        this.group = group;
    }

    @Override
    public void run(Continuation co){
        log.debug("{}: started", name);
        handler.handle(co, this);
    }

    public CoGroup group(){
        return group;
    }

    public CoHandler handler(){
        return handler;
    }

    public CoChannel handler(CoHandler handler){
        this.handler = handler;
        return this;
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
