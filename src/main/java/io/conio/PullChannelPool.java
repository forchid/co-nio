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
import io.conio.util.CoFuture;
import io.conio.util.IoUtils;
import io.conio.util.RtUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * <p>
 *     The coroutine pull mode channel pool.
 * </p>
 * @author little-pan
 * @since 2018-08-19
 */
public class PullChannelPool implements Closeable {
    final static Logger log = LoggerFactory.getLogger(PullChannelPool.class);

    private PullChannelPool(){}

    private String name = "PullChanPool";
    private int maxPoolSize = (RtUtils.PROCESSORS << 2) + 1;
    private long waitTime = 30000L;
    private boolean closed;

    private Map<InetSocketAddress, SaPool> saPools = new HashMap<>();

    public String getName(){
        return name;
    }

    public int getMaxPoolSize(){
        return maxPoolSize;
    }

    public long getWaitTime(){
        return waitTime;
    }

    public boolean isOpen(){
        return (!closed);
    }

    public CoFuture<PullCoChannel> getChannel(Continuation co, InetSocketAddress sa, PriotityKey priotityKey){
        if(!isOpen()){
            throw new IllegalStateException(name+" closed");
        }
        SaPool saPool = saPools.get(sa);
        if(saPool == null){
            saPool = new SaPool(this, sa);
            saPools.put(sa, saPool);
        }
        final CoFuture<PullCoChannel> future = saPool.getChannel(co, priotityKey);
        return future;
    }

    public CoFuture<PullCoChannel> getChannel(Continuation co, InetSocketAddress sa){
        return getChannel(co, sa, PriotityKey.SINGLE);
    }

    @Override
    public void close(){
        if(!isOpen()){
            log.debug("{}: closed", name);
            return;
        }
        this.closed = true;
        final Iterator<Map.Entry<InetSocketAddress, SaPool>> i = saPools.entrySet().iterator();
        for(;i.hasNext(); i.remove()){
            final Map.Entry<InetSocketAddress, SaPool> e = i.next();
            final SaPool saPool = e.getValue();
            saPool.close();
        }
        log.info("{}: Closed", name);
    }

    public final static Builder newBuilder(){
        return new Builder();
    }

    static class SaPool implements Closeable {
        final static Logger log = LoggerFactory.getLogger(SaPool.class);

        private final PullChannelPool parentPool;
        private final InetSocketAddress address;

        private Map<PriotityKey, Queue<PooledChannel>> pool = new HashMap<>();
        private int poolSize;
        private Queue<CoRunner> waiters = new LinkedList<>();

        public SaPool(PullChannelPool parentPool, InetSocketAddress address){
            this.parentPool = parentPool;
            this.address = address;
        }

        public CoFuture<PullCoChannel> getChannel(Continuation co, PriotityKey priotityKey){
            Queue<PooledChannel> queue = pool.get(priotityKey);
            if(queue == null){
                queue = new LinkedList<>();
                pool.put(priotityKey, queue);
            }

            final CoRunner waiter = (CoRunner)co.getContext();
            if(waiter == null){
                throw new IllegalArgumentException("No continuation context");
            }
            for(;;){
                if(!parentPool.isOpen()){
                    throw new IllegalStateException(parentPool.name+" closed");
                }
                final PullCoChannel chan = queue.poll();
                if(chan != null){
                    return newCoFuture(waiter, chan);
                }

                final Iterator<Map.Entry<PriotityKey, Queue<PooledChannel>>> i = pool.entrySet().iterator();
                for(;i.hasNext();){
                    final Map.Entry<PriotityKey, Queue<PooledChannel>> e = i.next();
                    if(e.getKey().equals(priotityKey)){
                        continue;
                    }
                    final PullCoChannel c = e.getValue().poll();
                    if(c == null){
                        continue;
                    }
                    return newCoFuture(waiter, c);
                }

                log.debug("{}: No free channel in this pool - poolSize = {}", address, poolSize);
                if(poolSize < parentPool.maxPoolSize){
                    break;
                }

                log.debug("{}: {} waits for free channel in this pool - poolSize = {}", address, waiter, poolSize);
                waiters.offer(waiter);
                co.suspend();
            }

            ++poolSize;
            boolean inited = false;
            try {
                final Queue<PooledChannel> chanQueue = queue;
                final SaPool self = this;
                final CoGroup group = waiter.group();

                log.debug("{}: {} builds a channel in this pool - poolSize = {}", address, waiter, poolSize);
                CoGroup.CoFutureImpl<PullCoChannel> future = group.connect(waiter, address);
                future.addListener((coChan, cause) -> {
                    boolean failed = true;
                    try{
                        log.debug("{}: {} connection completed - poolSize = {}", address, waiter, poolSize);
                        if(cause != null){
                            log.warn("Connection exception", cause);
                            return;
                        }
                        final PooledChannel poChan = new PooledChannel(self, priotityKey, coChan);
                        chanQueue.offer(poChan);
                        future.setValue(poChan);
                        log.debug("{}: {} connection success - poolSize = {}", address,  waiter, poolSize);
                        failed = false;
                    }finally {
                        if(failed){
                            --poolSize;
                            IoUtils.close(coChan);
                            final CoRunner another = waiters.poll();
                            if(another != null){
                                another.resume();
                            }
                        }
                    }

                }); // Co future listener

                inited = true;
                return future;
            } finally {
                if(!inited){
                    --poolSize;
                    final CoRunner another = waiters.poll();
                    if(another != null){
                        another.resume();
                    }
                }
            }
        }

        CoFuture<PullCoChannel> newCoFuture(CoRunner waiter, PullCoChannel chan){
            CoGroup.CoFutureImpl<PullCoChannel> future = new CoGroup.CoFutureImpl<>(waiter);
            future.setValue(chan).run();
            log.debug("{}: {} acquires channel {} from this pool", address, waiter, chan.name);
            return future;
        }

        @Override
        public void close(){
            final Iterator<Map.Entry<PriotityKey, Queue<PooledChannel>>> i = pool.entrySet().iterator();
            for(;i.hasNext(); i.remove()){
                final Map.Entry<PriotityKey, Queue<PooledChannel>> e = i.next();
                for(;;){
                    final PooledChannel pooled = e.getValue().poll();
                    if(pooled == null){
                        break;
                    }
                    close(pooled.wrappedChan());
                }
            }
        }

       final void close(CoChannel wrappedChan){
           wrappedChan.close();
            --poolSize;
            final CoRunner waiter = waiters.poll();
            if(waiter != null){
                waiter.resume();
            }
        }

        static class PooledChannel extends PullCoChannel {
            final static Logger log = LoggerFactory.getLogger(PooledChannel.class);

            final SaPool saPool;
            final PriotityKey priotityKey;

            public PooledChannel(SaPool saPool, PriotityKey priotityKey, PullCoChannel chan){
                super(chan);
                this.saPool = saPool;
                this.priotityKey = priotityKey;
            }

            @Override
            public int read(Continuation co, ByteBuffer dst) throws IOException {
                return wrappedChan().read(co, dst);
            }

            @Override
            public int write(Continuation co, ByteBuffer src) throws IOException {
                return wrappedChan().write(co, src);
            }

            @Override
            public boolean isOpen() {
                return wrappedChan().isOpen();
            }

            @Override
            public void close() {
                if(!saPool.parentPool.isOpen()){
                    saPool.close(wrappedChan());
                    return;
                }
                final Queue<PooledChannel> subPool = saPool.pool.get(priotityKey);
                subPool.offer(this);
                log.debug("{}: release channel {} into this pool", saPool.address, name);
                final CoRunner waiter = saPool.waiters.poll();
                if(waiter != null){
                    waiter.resume();
                }
            }

            CoChannel wrappedChan(){
                return (PullCoChannel)wrapped;
            }

        }// PooledChannel

    }// SaPool

    public interface PriotityKey {
        PriotityKey SINGLE = new PriotityKey() {};
    }// PriotityKey

    public static class Builder {

        private PullChannelPool pool;

        Builder(){
            pool = new PullChannelPool();
        }

        public Builder setName(String name){
            pool.name = name;
            return this;
        }

        public Builder setMaxPoolSize(int maxPoolSize){
            if(maxPoolSize < 1){
                throw new IllegalArgumentException("maxPoolSize " + maxPoolSize);
            }
            pool.maxPoolSize = maxPoolSize;
            return this;
        }

        public Builder setWaitTime(long waitTime){
            if(waitTime < 1){
                throw new IllegalArgumentException("waitTime " + waitTime);
            }
            pool.waitTime = waitTime;
            return this;
        }

        public PullChannelPool build(){
            log.info("{}: Started - maxPoolSize = {}, waitTime = {}ms",
                    pool.name, pool.maxPoolSize, pool.waitTime);
            return pool;
        }

    }// Builder

}
