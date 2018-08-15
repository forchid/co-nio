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
        final FactorialRequest request;
        try {
            request = FactorialCodec.decodeRequest(co, buffer);
        }catch(final EOFException e){
            return true;
        }

        // *ClassCastException thrown when processing logic enters the "if" statement, instrumentation bug?*
        //if(from < 1 || to < 1 || from > to){
        //    final String error = String.format("[%d, %d] out of range", from, to);
        //    result = ByteBuffer.wrap(error.getBytes(encoding));
        //    status = 0x1;
        //}else{
            // execute computation task in worker thread instead of in coroutine!
            final CoFuture<FactorialResponse> f = channel.execute(() -> {
                if(request.from < 1 || request.to < 1 || request.from > request.to){
                    final String error = String.format("[%d, %d] out of range", request.from, request.to);
                    return new FactorialResponse(error);
                }
                BigInteger factor = new BigInteger(request.from+"");
                for(int i = request.from + 1; i <= request.to; ++i){
                    factor = factor.multiply(new BigInteger(i+""));
                }
                return new FactorialResponse(factor);
            });

            FactorialResponse response = null;
            try {
                response = f.get(co);
            }catch(final ExecutionException e){
                response = new FactorialResponse(e.getCause().getMessage());
            }
        //}

        FactorialCodec.encodeResponse(co, buffer, response);
        return false;
    }

}
