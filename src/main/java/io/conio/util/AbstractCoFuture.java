package io.conio.util;

import com.offbynull.coroutines.user.Continuation;
import io.conio.CoRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * <p>
 * The abstract future that waits in a coroutine.
 * </p>
 * @author little-pan
 * @since 2018-08-19
 */
public abstract class AbstractCoFuture<V> implements CoFuture<V> {
    final static Logger log = LoggerFactory.getLogger(AbstractCoFuture.class);

    protected CoRunner waiter;
    protected boolean waited;

    protected V value;
    protected Throwable cause;

    protected List<CoFutureListener<V>> listeners;

    protected AbstractCoFuture(){}

    protected AbstractCoFuture(CoRunner waiter){
        this.waiter = waiter;
    }

    @Override
    public V get(Continuation co)throws ExecutionException{
        if(waiter != co.getContext()){
            throw new IllegalArgumentException("Continuation context not this future source");
        }
        if(!isDone()){
            waited = true;
            co.suspend();
            waited = false;
        }
        log.debug("{}: value = {}, cause = {}", waiter, value, cause);
        if(cause != null){
            if(cause instanceof  ExecutionException){
                throw (ExecutionException)cause;
            }
            throw new ExecutionException(cause);
        }
        return value;
    }

    protected abstract void setDone(boolean done);

    public CoFuture<V> setCause(Throwable cause){
        this.cause = cause;
        setDone(true);
        return this;
    }

    public CoFuture<V> setValue(V value){
        this.value = value;
        setDone(true);
        return this;
    }

    @Override
    public void addListener(CoFutureListener<V> listener){
        if(listeners == null){
            listeners = new ArrayList<>(2);
        }
        listeners.add(listener);
    }

}
