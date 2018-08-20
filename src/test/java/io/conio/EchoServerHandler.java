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
import io.conio.util.IoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

public class EchoServerHandler implements CoHandler {
    final static Logger log = LoggerFactory.getLogger(EchoServerHandler.class);

    final ByteBuffer buffer;

    public EchoServerHandler(){
        this(8192);
    }

    public EchoServerHandler(final int bufferSize){
        buffer = ByteBuffer.allocate(bufferSize);
    }

    @Override
    public void handle(Continuation co) {
        final PushCoChannel channel = (PushCoChannel)co.getContext();
        try{
            for(;!channel.group().isShutdown();){
                final int n = channel.read(co, buffer);
                if(n == -1){
                    break;
                }
                //log.debug("{}: recv {} bytes", channel.name, n);
                buffer.flip();
                for(;buffer.hasRemaining();) {
                    final int i = channel.write(co, buffer);
                    //log.debug("{}: send {} bytes", channel.name, i);
                }
                buffer.clear();
            }
        }catch(final IOException e){
            log.warn("IO error", e);
        }finally {
            IoUtils.close(channel);
        }
    }

}
