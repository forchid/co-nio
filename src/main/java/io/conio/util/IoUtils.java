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

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

public final class IoUtils {

    private IoUtils(){}

    public final static void close(Closeable closeable){
        if(closeable != null){
            try{
                closeable.close();
            }catch (IOException e){}
        }
    }

    public final static ByteBuffer copyToInBuffer(ByteBuffer src, ByteBuffer unused){
        if(unused == src){
            return unused;
        }
        if(src != null){
            unused.put(src);
        }
        unused.flip();
        return unused;
    }

    public final static ByteBuffer copyToOutBuffer(ByteBuffer src, ByteBuffer unused){
        if(unused == src){
            return unused;
        }
        if(src != null){
            unused.put(src);
        }
        return unused;
    }

    public final static String dumphex(final ByteBuffer buffer){
        final ByteBuffer slice = buffer.slice();
        final StringBuilder buf = new StringBuilder();
        int i = 0;
        for(; slice.hasRemaining(); ){
            final byte b = slice.get();
            if(i > 0){
                buf.append(' ');
            }
            buf.append(String.format("%02X", b));
            if(i == 15){
                i = 0;
                if(slice.hasRemaining()) {
                    buf.append('\n');
                }
                continue;
            }
            ++i;
        }
        return buf.toString();
    }

}
