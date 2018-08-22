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
 *     The ping-pong heartbeat server codec.
 * </p>
 * @author little-pan
 * @since 0.0.1-2018-08-22
 */
public class HeartbeatClientCodec extends PullChannelPool.HeartbeatCodec {

    public final static String ENCODING = ChannelCodec.ASCII;

    public final static byte CMD_PING = 0x02;

    public HeartbeatClientCodec(){}

    @Override
    public ByteBuffer ping(Continuation co) throws IOException {
        final ByteBuffer ping = ByteBuffer.allocate(5);
        ping.put(CMD_PING);
        ping.put("ping".getBytes(ENCODING));
        ping.flip();
        encode(co, ping);
        return decode(co);
    }

    @Override
    public ByteBuffer decode(Continuation co) throws IOException {
        final PullCoChannel chan = (PullCoChannel)co.getContext();
        // pong
        final int len = 4;
        final ByteBuffer ib = chan.inBuffer();
        ib.clear();
        for(; ib.position() < len;){
            final int n = chan.read(co, ib);
            if(n == -1){
                throw new EOFException("Peer closed");
            }
        }
        ib.flip();
        final ByteBuffer message = ByteBuffer.allocate(len);
        message.put(ib);
        message.flip();
        ib.clear();

        final String pong = new String(message.array(), 0, len, ENCODING);
        if("pong".equals(pong)){
            return message;
        }
        throw new IOException("Not pong message");
    }

}
