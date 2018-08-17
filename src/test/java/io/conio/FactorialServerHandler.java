package io.conio;

import com.offbynull.coroutines.user.Continuation;
import io.conio.util.CoFuture;
import io.conio.util.IoUtils;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.io.EOFException;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;

public class FactorialServerHandler implements CoHandler {
    final static Logger log = LoggerFactory.getLogger(FactorialServerHandler.class);
    protected final ByteBuffer buffer = ByteBuffer.allocate(8192);

    public FactorialServerHandler(){

    }

    @Override
    public void handle(Continuation co) {
        final CoChannel channel = (CoChannel)co.getContext();
        try{
            final CoGroup group = channel.group();
            for(;!group.isShutdown();){
                final boolean exit = calc(co);
                if(exit){
                    break;
                }
            }
        }catch (final Throwable cause){
            log.warn("Calc error", cause);
        }finally {
            cleanup(channel);
        }
    }

    protected void cleanup(final CoChannel channel){
        IoUtils.close(channel);
    }

    protected boolean calc(Continuation co)throws IOException {
        final FactorialRequest request;
        try {
            request = FactorialCodec.decodeRequest(co, buffer);
        }catch(final EOFException e){
            return true;
        }

        final FactorialResponse response;
        // *ClassCastException thrown when processing enters the "if" statement and using "else", instrumentation bug?*
        if(request.from < 1 || request.to < 1 || request.from > request.to){
            final String error = String.format("[%d, %d] out of range", request.from, request.to);
            response = new FactorialResponse(error);
            FactorialCodec.encodeResponse(co, buffer, response);
            return false;
        }

        response = doCalc(co, request);
        FactorialCodec.encodeResponse(co, buffer, response);
        return false;
    }

    protected FactorialResponse doCalc(Continuation co, final FactorialRequest request){
        log.debug("Calc begin: request {}", request);
        final CoChannel channel = (CoChannel)co.getContext();

        // execute computation task in worker thread instead of in coroutine!
        final CoFuture<FactorialResponse> f = channel.execute(() -> {
            // Reserved test for instrumentation bug
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

        try {
            log.debug("Calc: request {}", request);
            FactorialResponse response = f.get(co);
            log.debug("Calc end: factor {}", response.factor);
            return response;
        }catch(final ExecutionException e){
            log.warn("Calc error", e);
            return new FactorialResponse("Calc error");
        }
    }

}
