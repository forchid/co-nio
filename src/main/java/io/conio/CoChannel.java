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

import java.io.IOException;
import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.concurrent.Callable;

import com.offbynull.coroutines.user.Continuation;
import io.conio.util.CoFuture;
import io.conio.util.ScheduledCoFuture;

/**
 * <p>
 * The channel based on coroutine.
 * </p>
 * @since 2018-08-09
 * @author little-pan
 */
public interface CoChannel extends Closeable {

    int id();
    String name();

    CoGroup group();

    <V> CoFuture<V> execute(final Callable<V> callable);

    ScheduledCoFuture<?> schedule(CoHandler handler, final long delay);
    ScheduledCoFuture<?> schedule(CoHandler handler, long initialDelay, long period);

    int read(Continuation co, ByteBuffer dst) throws IOException;
    int write(Continuation co, ByteBuffer src) throws IOException;

    ByteBuffer inBuffer();
    CoChannel inBuffer(ByteBuffer buffer);

    ByteBuffer outBuffer();
    CoChannel outBuffer(ByteBuffer buffer);

    default ByteBuffer allocate(int size){
        return ByteBuffer.allocate(size);
    }

    boolean isOpen();

    @Override
    void close();

}
