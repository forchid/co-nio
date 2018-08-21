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
import java.math.BigInteger;
import java.nio.ByteBuffer;

public class FactorialCodec {
    final static Logger log = LoggerFactory.getLogger(FactorialCodec.class);

    public final static String encoding = ChannelCodec.ASCII;

    public final static FactorialRequest decodeRequest(Continuation co, ByteBuffer buffer)throws IOException {
        final CoChannel channel = (CoChannel)co.getContext();

        // ping?
        buffer.clear();
        buffer.limit(1);
        int n = channel.read(co, buffer);
        if(n == -1){
            throw new EOFException("Peer closed");
        }
        buffer.flip();
        if(CoProxyHeartbeatCodec.CMD_PING == (buffer.get() & 0xff)){
            buffer.clear();
            buffer.limit(4);
            for(;buffer.position() < 4;){
                n = channel.read(co, buffer);
                if(n == -1){
                    throw new EOFException("Peer closed");
                }
            }
            buffer.flip();
            final String ping = new String(buffer.array(), 0, 4, ChannelCodec.ASCII);
            if("ping".equals(ping)){
                log.info("{}: Receive {}", channel.name(), ping);
                channel.write(co, ByteBuffer.wrap("pong".getBytes(ChannelCodec.ASCII)));
                return null;
            }
            throw new IOException("Not ping message");
        }
        buffer.clear();

        // Receive: From(4), To(4)
        for(;buffer.position() < 8;){
            final int i = channel.read(co, buffer);
            if(i == -1){
                throw new EOFException("Peer closed");
            }
        }
        buffer.flip();
        final int from = buffer.getInt();
        final int to   = buffer.getInt();
        buffer.clear();

        final FactorialRequest request = new FactorialRequest(from, to);
        request.size = 8;
        return request;
    }

    public final static int encodeRequest(Continuation co, ByteBuffer buffer,
                                          FactorialRequest request)throws IOException {
        final CoChannel channel = (CoChannel)co.getContext();
        if(channel == null){
            throw new NullPointerException("channel null");
        }
        // CMD_CALC: 0x01
        buffer.clear();
        buffer.put((byte)1);
        buffer.flip();
        channel.write(co, buffer);
        buffer.clear();

        // Send: From(4), To(4)
        buffer.putInt(request.from);
        buffer.putInt(request.to);
        buffer.flip();
        channel.write(co, buffer);
        buffer.clear();
        return request.size = 9;
    }

    public final static FactorialResponse decodeResponse(Continuation co, ByteBuffer buffer)throws IOException {
        final CoChannel channel = (CoChannel)co.getContext();

        // Receive: LEN(4), status, result
        for(;buffer.position() < 4;){
            final int i = channel.read(co, buffer);
            if(i == -1){
                throw new EOFException("Peer closed");
            }
        }
        buffer.flip();
        final int len = buffer.getInt();

        ByteBuffer buf = buffer.compact();
        for(;buf.position() < len;){
            final int i = channel.read(co, buf);
            if(i == -1){
                throw new EOFException();
            }
            if(buf.remaining() == 0){
                ByteBuffer b = ByteBuffer.allocate(buf.capacity()<<1);
                buf.flip();
                b.put(buf);
                buf = b;
            }
        }
        buf.flip();

        final int status = buf.get() & 0xff;
        final byte[] a = buf.array();
        final String result = new String(a, 1, buf.limit()-1, encoding);
        if(status != 0){
            final FactorialResponse response = new FactorialResponse(result);
            response.size = len + 4;
            return response;
        }
        final FactorialResponse response = new FactorialResponse(new BigInteger(result));
        response.size = len + 4;
        return response;
    }

    public final static int encodeResponse(Continuation co, ByteBuffer buffer,
                                           FactorialResponse response)throws IOException {
        final CoChannel channel = (CoChannel)co.getContext();

        final ByteBuffer result;
        final byte status;
        if(response.error == null){
            final BigInteger factor = response.factor;
            result = ByteBuffer.wrap(factor.toString().getBytes(encoding));
            status = 0x0;
        } else {
            final String error = response.error;
            result = ByteBuffer.wrap(error.getBytes(encoding));
            status = 0x01;
        }

        // Send: LEN(4), status, result
        buffer.putInt(result.capacity() + 1);
        buffer.put(status);
        buffer.flip();
        channel.write(co, buffer);
        channel.write(co, result);

        return response.size = result.capacity() + 5;
    }

}
