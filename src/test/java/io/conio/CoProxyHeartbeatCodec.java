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

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * <p>
 *     The ping-pong heartbeat codec.
 * </p>
 * @author little-pan
 * @since 0.0.1-2018-08-21
 */
public class CoProxyHeartbeatCodec extends PullChannelPool.HeartbeatCodec {

    final static byte CMD_PING = 0x02;

    public CoProxyHeartbeatCodec(){
        super(ByteBuffer.allocate(4));
    }

    @Override
    protected ByteBuffer message() {
        ByteBuffer message = ByteBuffer.allocate(5);
        try{
            message.put(CMD_PING);
            message.put("ping".getBytes(ASCII));
            message.flip();
        }catch (final IOException e){
            // ignore
        }
        return message;
    }

    @Override
    public ByteBuffer decode(Continuation co, ByteBuffer buffer) throws IOException {
        // pong
        final int len = 4;
        final PullCoChannel chan = (PullCoChannel)co.getContext();
        for(;buffer.position() < len;){
            final int n = chan.read(co, buffer);
            if(n == -1){
                throw new EOFException("Peer closed");
            }
        }
        buffer.flip();
        buffer.limit(len);
        final ByteBuffer pong = ByteBuffer.allocate(len);
        pong.put(buffer);
        buffer.clear();
        return pong;
    }

}
