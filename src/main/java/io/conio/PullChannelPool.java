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
import io.conio.util.ScheduledCoFuture;
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
 *     The coroutine pull mode channel pool in a CoGroup.
 * </p>
 * @author little-pan
 * @since 2018-08-19
 */
public class PullChannelPool implements CoHandler, Closeable {
    final static Logger log = LoggerFactory.getLogger(PullChannelPool.class);

    private String name = "PullChanPool";
    private int maxSize = (RtUtils.PROCESSORS << 2) + 1;
    private long maxWait = 30000L;         // ms
    private long heartbeatInterval = 30L;  // unit s, no heartbeat when this value < 1
    private boolean closed;

    // Heartbeat properties
    private ScheduledCoFuture<?> heartbeatFuture;
    private HeartbeatCodec heartbeatCodec;
    private boolean heartbeating;

    private Map<InetSocketAddress, SaPool> saPools = new HashMap<>();

    private final CoGroup group;

    private PullChannelPool(CoGroup group){
        this.group = group;
    }

    public String getName(){
        return name;
    }

    public int getMaxSize(){
        return maxSize;
    }

    public long getMaxWait(){
        return maxWait;
    }

    public long getHeartbeatInterval(){
        return heartbeatInterval;
    }

    public HeartbeatCodec getHeartbeatCodec(){
        return heartbeatCodec;
    }

    public boolean isOpen(){
        return (!closed);
    }

    public CoFuture<PullCoChannel> getChannel(Continuation co, InetSocketAddress sa, PriorityKey priorityKey){
        if(!group.inGroup()){
            throw new IllegalStateException("Current thread not in CoGroup " + group.getName());
        }
        if(!isOpen()){
            throw new IllegalStateException(name+" closed");
        }
        SaPool saPool = saPools.get(sa);
        if(saPool == null){
            saPool = new SaPool(this, sa);
            saPools.put(sa, saPool);
        }
        final CoFuture<PullCoChannel> future = saPool.getChannel(co, priorityKey);
        return future;
    }

    public CoFuture<PullCoChannel> getChannel(Continuation co, InetSocketAddress sa){
        return getChannel(co, sa, PriorityKey.SINGLE);
    }

    /**
     * <p>
     *    Heartbeat entrance.
     * </p>
     * @param co the continuation
     * @since 0.0.1-2018-08-21
     * @auhtor little-pan
     */
    @Override
    public void handle(Continuation co) {
        if(!isOpen()){
            final ScheduledCoFuture<?> f = heartbeatFuture;
            if(f != null){
                log.info("{}: Closed - cancel heartbeat", name);
                f.cancel(true);
                heartbeatFuture = null;
            }
            return;
        }
        final PushCoRunner coRunner = (PushCoRunner)co.getContext();
        if(heartbeating){
            log.warn("{}: {} - The last heartbeat pending", name, coRunner.name);
            return;
        }

        try {
            heartbeating = true;
            log.debug("{}: {} - Heartbeat begin", name, coRunner.name);
            heartbeat(co);
        }catch (final Exception e){
            log.warn(name + ": " + coRunner.name + " - Heartbeat error", e);
        }finally {
            heartbeating = false;
            log.debug("{}: {} - Heartbeat end", name, coRunner.name);
        }
    }

    private void heartbeat(Continuation co) {
        final long currentTime = System.currentTimeMillis();
        final Set<InetSocketAddress> addresses = new HashSet<>(saPools.keySet());
        final Set<SaPool.PooledChannel> beated = new HashSet<>();
        log.debug("{}: addresses = {}", name, addresses);

        for(final InetSocketAddress sa : addresses){
            final SaPool saPool = saPools.get(sa);
            if(saPool == null){
                continue;
            }
            if(saPool.poolSize == 0){
                // Remove no any channel pool for economical memory usage
                saPools.remove(sa);
                continue;
            }
            for(;;){
                SaPool.PooledChannel pooled = null;
                boolean failed = true;
                try {
                    pooled = saPool.getChannel(beated, currentTime);
                    if (pooled == null) {
                        failed = false;
                        break;
                    }
                    final CoFuture<ByteBuffer> f = doHeartbeat(pooled);
                    f.get(co);
                    pooled.close(); // Release it after test
                    failed = false;
                }catch (final ExecutionException e){
                    Throwable cause = e.getCause();
                    if(cause instanceof RuntimeException && cause.getCause() != null){
                        cause = cause.getCause();
                    }
                    log.warn(name + ": Do heartbeat error", cause);
                    continue;
                }finally {
                    if(failed){
                        IoUtils.close(pooled);
                        for(final SaPool.PooledChannel chan: beated){
                            IoUtils.close(chan);
                        }
                    }
                }
                beated.add(pooled);
            }
            beated.clear();
        }
    }

    private CoFuture<ByteBuffer> doHeartbeat(SaPool.PooledChannel pooled) {
        final HeartbeatCodec codec = heartbeatCodec;
        return pooled.execute((c) -> {
            try {
                return codec.heartbeat(c);
            }catch(final IOException e){
                throw new RuntimeException(e);
            }
        });
    }

    final void setHeartbeatFuture(ScheduledCoFuture<?> heartbeatFuture){
        this.heartbeatFuture = heartbeatFuture;
    }

    @Override
    public void close(){
        if(!group.inGroup()){
            throw new IllegalStateException("Current thread not in CoGroup " + group.getName());
        }
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

    final static Builder newBuilder(CoGroup group){
        return new Builder(group);
    }

    static class SaPool implements Closeable {
        final static Logger log = LoggerFactory.getLogger(SaPool.class);

        private final PullChannelPool parentPool;
        private final InetSocketAddress address;

        private Map<PriorityKey, Queue<PooledChannel>> pool = new HashMap<>();
        private int poolSize;
        private Queue<CoRunner> waiters = new LinkedList<>();

        public SaPool(PullChannelPool parentPool, InetSocketAddress address){
            this.parentPool = parentPool;
            this.address = address;
        }

        public CoFuture<PullCoChannel> getChannel(Continuation co, PriorityKey priorityKey){
            Queue<PooledChannel> queue = pool.get(priorityKey);
            if(queue == null){
                queue = new LinkedList<>();
                pool.put(priorityKey, queue);
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

                final Iterator<Map.Entry<PriorityKey, Queue<PooledChannel>>> i = pool.entrySet().iterator();
                for(;i.hasNext();){
                    final Map.Entry<PriorityKey, Queue<PooledChannel>> e = i.next();
                    if(e.getKey().equals(priorityKey)){
                        continue;
                    }
                    final PullCoChannel c = e.getValue().poll();
                    if(c == null){
                        continue;
                    }
                    return newCoFuture(waiter, c);
                }

                log.debug("{}: No free channel in this pool - poolSize = {}", address, poolSize);
                if(poolSize < parentPool.maxSize){
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
                        final PooledChannel poChan = new PooledChannel(self, priorityKey, coChan);
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

        PooledChannel getChannel(final Set<PooledChannel> beated, final long currentTime){
            final Iterator<Map.Entry<PriorityKey, Queue<PooledChannel>>> i = pool.entrySet().iterator();
            final long bhi = parentPool.heartbeatInterval * 1000L;
            final Queue<PooledChannel> backed = new LinkedList<>();

            for(; i.hasNext(); ){
                final Map.Entry<PriorityKey, Queue<PooledChannel>> e = i.next();
                final Queue<PooledChannel> queue = e.getValue();
                for(;;){
                    final PooledChannel c = queue.poll();
                    if(c == null){
                        break;
                    }
                    if(beated.contains(c) || currentTime - c.lastAccessTime < bhi){
                        backed.offer(c);
                        continue;
                    }
                    queue.addAll(backed);
                    backed.clear();
                    log.debug("{}: heartbeat acquires channel {} from this pool", address, c.name);
                    return c;
                }
                queue.addAll(backed);
                backed.clear();
            }
            return null;
        }

        @Override
        public void close(){
            final Iterator<Map.Entry<PriorityKey, Queue<PooledChannel>>> i = pool.entrySet().iterator();
            for(;i.hasNext(); i.remove()){
                final Map.Entry<PriorityKey, Queue<PooledChannel>> e = i.next();
                for(;;){
                    final PooledChannel pooled = e.getValue().poll();
                    if(pooled == null){
                        break;
                    }
                    close(pooled.wrappedChan());
                }
            }
            log.info("{}: {} closed - pollSize = {}", parentPool.name, address, poolSize);
        }

       final void close(CoChannel wrappedChan){
           if(!wrappedChan.isOpen()){
               return;
           }
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
            final PriorityKey priorityKey;

            long lastAccessTime;
            private boolean ioe;

            public PooledChannel(SaPool saPool, PriorityKey priorityKey, PullCoChannel chan){
                super(chan);
                this.saPool = saPool;
                this.priorityKey = priorityKey;
            }

            @Override
            public int read(Continuation co, ByteBuffer dst) throws IOException {
                boolean failed = true;
                try{
                    final int i = wrappedChan().read(co, dst);
                    if(i != 0){
                        this.lastAccessTime = System.currentTimeMillis();
                        if(i == -1){
                            return i;
                        }
                    }
                    failed = false;
                    return i;
                }finally {
                    if(failed){
                        this.ioe = true;
                    }
                }
            }

            @Override
            public int write(Continuation co, ByteBuffer src) throws IOException {
                boolean failed = true;
                try{
                    final int n = wrappedChan().write(co, src);
                    if(n > 0){
                        this.lastAccessTime = System.currentTimeMillis();
                    }
                    failed = false;
                    return n;
                } finally {
                    if(failed){
                        this.ioe = true;
                    }
                }
            }

            @Override
            public boolean isOpen() {
                return wrappedChan().isOpen();
            }

            @Override
            public void close() {
                if(this.ioe || (!saPool.parentPool.isOpen())){
                    saPool.close(wrappedChan());
                    return;
                }
                final Queue<PooledChannel> subPool = saPool.pool.get(priorityKey);
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

            final long getLastAccessTime(){
                return lastAccessTime;
            }

        }// PooledChannel

    }// SaPool

    public interface PriorityKey {
        PriorityKey SINGLE = new PriorityKey() {};
    }// PriorityKey

    public static abstract class HeartbeatCodec implements ChannelCodec<ByteBuffer, ByteBuffer> {
        protected ByteBuffer buffer;

        protected HeartbeatCodec(ByteBuffer buffer){
            this.buffer = buffer;
        }

        public ByteBuffer heartbeat(Continuation co) throws IOException {
            encode(co, buffer, message());
            return decode(co, buffer);
        }

        @Override
        public void encode(Continuation co, ByteBuffer buffer, ByteBuffer message)throws IOException {
            final PullCoChannel chan = (PullCoChannel)co.getContext();
            for(; message.hasRemaining(); ){
                chan.write(co, message);
            }
        }

        protected abstract ByteBuffer message();

    }// HeartbeatCodec

    static class Builder {

        private PullChannelPool pool;

        Builder(CoGroup group){
            pool = new PullChannelPool(group);
        }

        public Builder setName(String name){
            pool.name = name;
            return this;
        }

        public Builder setMaxSize(int maxSize){
            if(maxSize < 1){
                throw new IllegalArgumentException("maxSize " + maxSize);
            }
            pool.maxSize = maxSize;
            return this;
        }

        public Builder setMaxWait(long maxWait){
            if(maxWait < 1){
                throw new IllegalArgumentException("maxWait " + maxWait);
            }
            pool.maxWait = maxWait;
            return this;
        }

        public Builder setHeartbeatInterval(long heartbeatInterval){
            pool.heartbeatInterval = heartbeatInterval;
            return this;
        }

        public Builder setHeartbeatCodec(HeartbeatCodec heartbeatCodec){
            pool.heartbeatCodec = heartbeatCodec;
            return this;
        }

        public PullChannelPool build(){
            log.info("{}: Started - maxSize = {}, maxWait = {}ms, heartbeatInterval = {}s",
                    pool.name, pool.maxSize, pool.maxWait, pool.heartbeatInterval);
            return pool;
        }

    }// Builder

}
