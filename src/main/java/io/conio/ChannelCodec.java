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

import java.io.IOException;

/**
 * <p>
 *     Coroutine channel codec.
 * </p>
 * @author little-pan
 * @since 0.0.1-2018-08-21
 */
public interface ChannelCodec<I, O> {

    String UTF_8 = "UTF-8";
    String ASCII = "ascii";

    void encode(Continuation co, I in) throws IOException;

    O decode(Continuation co)throws IOException;

}
