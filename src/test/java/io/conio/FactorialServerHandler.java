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
    public void handle(Continuation co) {
        final CoChannel channel = (CoChannel)co.getContext();
        try{
            final CoGroup group = channel.group();
            for(;!group.isShutdown();){
                final boolean exit = calc(co, channel);
                if(exit){
                    break;
                }
            }
        }catch (final Throwable cause){
            log.warn("Calc error", cause);
        }finally {
            IoUtils.close(channel);
        }
    }

    private boolean calc(Continuation co, CoChannel channel)throws IOException {
        buffer.clear();

        // Receive: From(4), To(4)
        for(;buffer.position() < 8;){
            final int i = channel.read(co, buffer);
            if(i == -1){
                return true;
            }
        }
        buffer.flip();
        final int from = buffer.getInt();
        final int to   = buffer.getInt();
        buffer.clear();

        ByteBuffer result;
        byte status = 0x0;
        // *ClassCastException thrown when processing logic enters the "if" statement, instrumentation bug?*
        //if(from < 1 || to < 1 || from > to){
        //    final String error = String.format("[%d, %d] out of range", from, to);
        //    result = ByteBuffer.wrap(error.getBytes(encoding));
        //    status = 0x1;
        //}else{
            // execute computation task in worker thread instead of in coroutine!
            final CoFuture<FactorialResult> f = channel.execute(() -> {
                if(from < 1 || to < 1 || from > to){
                    final String error = String.format("[%d, %d] out of range", from, to);
                    return new FactorialResult(error);
                }
                BigInteger factor = new BigInteger(from+"");
                for(int i = from + 1; i <= to; ++i){
                    factor = factor.multiply(new BigInteger(i+""));
                }
                return new FactorialResult(factor);
            });
            try {
                final FactorialResult res = f.get(co);
                if(res.error == null){
                    final BigInteger factor = res.factor;
                    result = ByteBuffer.wrap(factor.toString().getBytes(encoding));
                }else {
                    final String error = res.error;
                    result = ByteBuffer.wrap(error.getBytes(encoding));
                    status = 0x01;
                }
            }catch(final ExecutionException e){
                final String error = e.getCause().getMessage();
                result = ByteBuffer.wrap(error.getBytes(encoding));
                status = 0x1;
            }
        //}

        // Send: LEN(4), status, result
        buffer.putInt(result.capacity() + 1);
        buffer.put(status);
        buffer.flip();
        channel.write(co, buffer);
        channel.write(co, result);
        return false;
    }

    static class FactorialResult {
        public final BigInteger factor;
        public final String error;

        public FactorialResult(BigInteger factor){
            this(factor, null);
        }

        public FactorialResult(String error){
            this(null, error);
        }

        protected FactorialResult(BigInteger factor, String error){
            this.factor = factor;
            this.error  = error;
        }

    }

}
