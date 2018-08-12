package io.conio;

import com.offbynull.coroutines.user.Continuation;
import io.conio.util.CoFuture;
import io.conio.util.IoUtils;

import java.io.EOFException;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

public class FactorialServerHandler implements CoHandler {

    protected final ByteBuffer buffer = ByteBuffer.allocate(8192);
    protected final String encoding = "ascii";

    public FactorialServerHandler(){

    }

    @Override
    public void handle(Continuation co, CoChannel channel) {
        try{
            final CoGroup group = channel.group();
            for(;!group.isShutdown();){
                final boolean exit = calc(co, channel);
                if(exit){
                    break;
                }
            }
        }catch (final IOException cause){
            log.warn("IO error", cause);
        }finally {
            IoUtils.close(channel);
        }
    }

    private boolean calc(Continuation co, CoChannel channel)throws IOException {
        buffer.clear();

        // Receive: N(4)
        for(;buffer.position() < 4;){
            final int i = channel.read(co, buffer);
            if(i == -1){
                return true;
            }
        }
        buffer.flip();
        final int n = buffer.getInt();
        buffer.clear();

        ByteBuffer result;
        byte status = 0x0;
        if(n < 1){
            final String error = String.format("%d out of range", n);
            result = ByteBuffer.wrap(error.getBytes(encoding));
            status = 0x1;
        }else{
            // execute computation task in worker thread instead of in coroutine!
            final CoFuture<BigInteger> f = channel.execute(() -> {
                BigInteger factor = BigInteger.ONE;
                for(int i = 2; i <= n; ++i){
                    factor = factor.multiply(new BigInteger(i+""));
                }
                return factor;
            });
            try {
                final BigInteger factor = f.get(co);
                result = ByteBuffer.wrap(factor.toString().getBytes(encoding));
            }catch(final ExecutionException e){
                final String error = e.getCause().getMessage();
                result = ByteBuffer.wrap(error.getBytes(encoding));
                status = 0x1;
            }
        }

        // Send: LEN(4), status, result
        buffer.putInt(result.capacity() + 1);
        buffer.put(status);
        buffer.flip();
        channel.write(co, buffer);
        channel.write(co, result);
        return false;
    }

}
