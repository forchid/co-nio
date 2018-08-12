package io.conio;

import com.offbynull.coroutines.user.Continuation;
import io.conio.util.IoUtils;

import java.io.EOFException;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;

public class FactorialClientHandler extends BaseTest implements CoHandler {

    final static BigInteger [] FACTORS = {
            new BigInteger("1"), new BigInteger("2"), new BigInteger("6"),
            new BigInteger("24"), new BigInteger("120"), new BigInteger("720"),
            new BigInteger("5040"), new BigInteger("40320"), new BigInteger("362880"),
            new BigInteger("3628800"), new BigInteger("39916800"), new BigInteger("479001600")
    };

    protected final ByteBuffer buffer = ByteBuffer.allocate(8192);
    protected final String encoding = "ascii";

    int maxlen = 0;

    public FactorialClientHandler(){

    }

    @Override
    public void handle(Continuation co, CoChannel channel) {
        int i = 1;
        BigInteger result = null;
        try{
            final CoGroup group = channel.group();
            for(;!group.isShutdown(); ++i){
                result = calc(co, channel, i);
                final BigInteger expect;
                if(i <= FACTORS.length && result.compareTo(FACTORS[i - 1]) != 0){
                    log.warn("Result error: {} expect {}", result, FACTORS[i - 1]);
                    break;
                }
                ++times;
            }
        }catch (final IOException cause){
            log.warn("IO error", cause);
        }finally {
            //log.info("{}!: max-length = {}", i, maxlen);
            IoUtils.close(channel);
        }
    }

    private BigInteger calc(Continuation co, CoChannel channel, final int n)throws IOException {
        buffer.clear();
        // Send: N(4)
        buffer.putInt(n);
        buffer.flip();
        channel.write(co, buffer);
        buffer.clear();
        bytes += 4;

        // Receive: LEN(4), status, result
        for(;buffer.position() < 4;){
            final int i = channel.read(co, buffer);
            if(i == -1){
                throw new EOFException();
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
        bytes += (n + 5);

        final int status = buf.get() & 0xff;
        final byte[] a = buf.array();
        final String result = new String(a, 1, buf.limit()-1, encoding);
        if(status != 0){
            throw new IOException(result);
        }
        if(maxlen < result.length()){
            maxlen = result.length();
        }
        return new BigInteger(result);
    }

}
