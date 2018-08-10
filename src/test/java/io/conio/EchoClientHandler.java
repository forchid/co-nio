package io.conio;

import com.offbynull.coroutines.user.Continuation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class EchoClientHandler implements CoHandler {
    final static Logger log = LoggerFactory.getLogger(EchoClientHandler.class);

    final ByteBuffer buffer;
    final byte[] data;

    // statistics
    long bytes = 0L, times = 0L;

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
    public void handle(Continuation co, CoChannel channel) {
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
            channel.close();
        }
    }

}
