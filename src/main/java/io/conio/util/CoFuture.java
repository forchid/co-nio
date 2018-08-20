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
package io.conio.util;

import com.offbynull.coroutines.user.Continuation;

import java.util.concurrent.ExecutionException;

/**
 * <p>
 * The future that waits in a coroutine.
 * </p>
 * @author little-pan
 * @since 2018-08-12
 */
public interface CoFuture<V> {

    V get(Continuation co)throws ExecutionException;

    boolean isDone();

    void addListener(CoFutureListener<V> listener);

}
