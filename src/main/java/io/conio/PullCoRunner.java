/*
 * Copyright (c) 2018, little-pan, All rights reserved.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3.0 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library.
 */
package io.conio;

import com.offbynull.coroutines.user.Continuation;
import io.conio.util.AbstractCoFuture;
import io.conio.util.CoCallable;
import io.conio.util.CoFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ExecutionException;

/**
 * <p>
 * The pull mode CoRunner.
 * </p>
 * @since 2018-08-18
 * @author little-pan
 */
public class PullCoRunner extends CoRunner {
    final static Logger log = LoggerFactory.getLogger(PullCoRunner.class);

    protected final PullCoRunner wrapped;

    private final Queue<CoFutureTask<?>> coQueue;
    private boolean idle;
    private boolean stopped;

    public PullCoRunner(final int id, CoGroup group){
        super(id, "pull-co-"+id, group);
        this.coQueue = new LinkedList<>();
        this.wrapped = null;
    }

    public PullCoRunner(final int id, String name, CoGroup group){
        super(id, name, group);
        this.coQueue = new LinkedList<>();
        this.wrapped = null;
    }

    public PullCoRunner(PullCoRunner wrapped){
        super(wrapped);
        this.coQueue = wrapped.coQueue;
        this.wrapped = wrapped;
    }

    /**
     * <p>
     * Execute the callable in the coroutine.
     * </p>
     * @param coCallable
     * @param <V>
     * @return the callable future
     */
    public <V> CoFuture<V> execute(final CoCallable<V> coCallable){
        final CoFutureTask<V> f = new CoFutureTask<>(coCallable);
        coQueue.offer(f);
        if(isIdle()){
            resume();
        }
        return f;
    }

    public boolean isStopped(){
        if(wrapped != null){
            return wrapped.stopped;
        }
        return stopped;
    }

    public void stop(){
        if(isStopped()){
            return;
        }
        if(wrapped != null){
            wrapped.stopped = true;
        }else{
            stopped = true;
        }
        resume();
    }

    @Override
    public void run(Continuation co){
        if(wrapped != null){
            throw new IllegalStateException("Can't run PullCoRunner wrapper");
        }

        co.setContext(this);
        log.debug("{}: Started", name);

        for(;!isStopped();) {
            handleCoTasks(co);
            try{
                if(!isStopped()){
                    idle = true;
                    co.suspend();
                }
            }finally {
                idle = false;
            }
        }
        coQueue.clear();
        stopped = true;
        log.debug("{}: Stopped", name);
    }

    private void handleCoTasks(Continuation co){
        for(;;){
            final CoFutureTask<?> f = coQueue.poll();
            if(f == null){
                break;
            }
            try {
                final CoCallable<?> callable = f.coCallable;
                final Object result = callable.call(co);
                f.setValue(result);
            }catch(final Throwable cause){
                f.setCause(cause);
            }
        }
    }

    public final boolean isIdle(){
        if(wrapped != null){
            return wrapped.idle;
        }
        return idle;
    }

    static class CoFutureTask<V> extends AbstractCoFuture<V> {
        final CoCallable<V> coCallable;

        private boolean done;

        public CoFutureTask(final CoCallable<V> coCallable){
            super(null);
            this.coCallable = coCallable;
        }

        @Override
        public V get(Continuation co)throws ExecutionException{
            waiter = (CoRunner)co.getContext();
            return super.get(co);
        }

        @Override
        @SuppressWarnings("unchecked")
        public CoFuture<V> setValue(Object value){
            super.setValue((V)value);
            return this;
        }

        @Override
        public boolean isDone() {
            return done;
        }

        @Override
        public void setDone(boolean done){
            this.done = done;
            if(waiter != null && waited){
                waiter.resume();
            }
        }

    }// CoFutureTask

}
