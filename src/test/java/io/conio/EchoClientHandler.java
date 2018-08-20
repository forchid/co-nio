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

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class EchoClientHandler extends BaseTest implements CoHandler {
    final static Logger log = LoggerFactory.getLogger(EchoClientHandler.class);

    final ByteBuffer buffer;
    final byte[] data;

    public EchoClientHandler(){
        this(8192);
    }

    public EchoClientHandler(final int bufferSize){
        buffer = ByteBuffer.allocate(bufferSize);
        data   = new byte[bufferSize];
        for(int i = 0, size = data.length; i < size; ++i){
            data[i] = (byte)i;
        }
    }

    @Override
    public void handle(Continuation co) {
        final PushCoChannel channel = (PushCoChannel)co.getContext();
        try{
            final ByteBuffer dbuf = ByteBuffer.wrap(data);
            final CoGroup group = channel.group();
            for(;!group.isShutdown();){
                for(;dbuf.hasRemaining();){
                    final int n = channel.write(co, dbuf);
                    bytes += n;
                    //log.debug("{}: send {} bytes", channel.name, n);
                }
                dbuf.flip();
                for(;buffer.hasRemaining();){
                    final int n = channel.read(co, buffer);
                    if(n == -1){
                        throw new EOFException("Server closed");
                    }
                    bytes += n;
                }
                buffer.flip();
                if(!Arrays.equals(data, buffer.array())){
                    throw new IOException("Packet malformed");
                }
                buffer.clear();
                ++times;
            }
        }catch(final IOException e){
            log.warn("IO error", e);
        }finally {
            IoUtils.close(channel);
        }
    }

}
