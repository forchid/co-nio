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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * <p>
 *     The ping-pong heartbeat client codec.
 * </p>
 * @author little-pan
 * @since 0.0.1-2018-08-21
 */
public class HeartbeatServerCodec implements ChannelCodec<ByteBuffer, ByteBuffer> {
    final static Logger log = LoggerFactory.getLogger(HeartbeatServerCodec.class);
    final static String ENCODING = HeartbeatClientCodec.ENCODING;

    public HeartbeatServerCodec(){

    }

    public ByteBuffer pong(Continuation co)throws IOException {
        final ByteBuffer ping = decode(co);
        final byte[] a = "pong".getBytes(ENCODING);
        final ByteBuffer pong = ByteBuffer.wrap(a);
        encode(co, pong);
        return ping;
    }

    @Override
    public void encode(Continuation co, ByteBuffer message) throws IOException {
        final CoChannel channel = (CoChannel)co.getContext();
        for(;message.hasRemaining();){
            channel.write(co, message);
        }
    }

    @Override
    public ByteBuffer decode(Continuation co) throws IOException {
        final PushCoChannel channel = (PushCoChannel)co.getContext();
        final ByteBuffer ib = channel.inBuffer();
        // ping
        final int len = 4;
        for(int n; ib.position() < len; ){
            n = channel.read(co, ib);
            if(n == -1){
                throw new EOFException("Peer closed");
            }
        }
        ib.flip();
        final ByteBuffer message = ByteBuffer.allocate(len);
        message.put(ib);
        message.flip();
        ib.clear();

        final String ping = new String(message.array(), 0, len, ENCODING);
        log.debug("{}: receive heartbeat {}", channel.name, ping);
        if("ping".equals(ping)){
            return message;
        }
        throw new IOException("Not ping message");
    }

}
