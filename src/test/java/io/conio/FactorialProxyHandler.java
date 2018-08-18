package io.conio;

import com.offbynull.coroutines.user.Continuation;
import io.conio.util.CoFuture;
import io.conio.util.IoUtils;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.io.IOException;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class FactorialProxyHandler extends FactorialServerHandler {
    final static Logger log = LoggerFactory.getLogger(FactorialProxyHandler.class);

    final InetSocketAddress backends[];
    protected PullCoChannel backendChans[];

    public FactorialProxyHandler(InetSocketAddress backends[]){
        this.backends = backends;
    }

    @Override
    protected FactorialResponse doCalc(final Continuation co, final FactorialRequest request){
        final PushCoChannel channel = (PushCoChannel)co.getContext();
        final CoGroup group = channel.group();
        final InetSocketAddress backends[] = this.backends;

        // 1. connect
        if(backendChans == null){
            final List<CoFuture<PullCoChannel>> cfutures = new ArrayList<>(backends.length);
            for(int i = 0; i < backends.length; ++i){
                final InetSocketAddress backend = backends[i];
                cfutures.add(group.connect(channel, backend));
            }
            backendChans = new PullCoChannel[backends.length];
            for(int i = 0; i < cfutures.size(); ++i){
                try {
                    final CoFuture<PullCoChannel> cf = cfutures.get(i);
                    backendChans[i] = cf.get(co);
                }catch(final ExecutionException e){
                    release(backendChans);
                    throw new RuntimeException(e);
                    //return new FactorialResponse(e.getCause().getMessage());
                }
            }
        }

        // 2. calculate
        final int range = request.to - request.from + 1;
        final int sizePerShard = range / backends.length;
        final int shards = (range >= backends.length ? backends.length: range % backends.length);
        log.debug("request: {}, shards: {}, sizePerShard: {}", request, shards, sizePerShard);
        final List<CoFuture<FactorialResponse>> rfutures = new ArrayList<>(shards);
        for(int from = request.from, i = 0; from <= request.to; ++i){
            final PullCoChannel chan = backendChans[i];
            final int start = from, to;
            if(i == shards - 1){
                final int rem = range % backends.length;
                to = from + sizePerShard + rem - 1;
                from += sizePerShard + rem;
            }else {
                to = from + sizePerShard - 1;
                from += sizePerShard;
            }
            final FactorialRequest req = new FactorialRequest(start, to);
            log.debug("Send to backend: request {}", req);
            final CoFuture<FactorialResponse> cf = chan.execute((c) -> {
                try{
                    final ByteBuffer buf = ByteBuffer.allocate(64);
                    FactorialCodec.encodeRequest(c, buf, req);
                    return FactorialCodec.decodeResponse(c, buf);
                }catch (final IOException e){
                    throw new RuntimeException(e);
                }
            });
            rfutures.add(cf);
        }
        final FactorialResponse responses[] = new FactorialResponse[rfutures.size()];
        for(int i = 0; i < responses.length; ++i){
            try{
                responses[i] = rfutures.get(i).get(co);
            }catch(final Throwable cause){
                log.warn("Calc error", cause);
                release(backendChans);
                final String error = cause.getMessage();
                return new FactorialResponse(error);
            }
        }

        // 3. merge
        BigInteger factor = responses[0].factor;
        log.debug("factor{} <- {}", 0, factor);
        for(int i = 1; i < responses.length; ++i){
            factor = factor.multiply(responses[i].factor);
            log.debug("factor{} <- {}", i, factor);
        }
        return new FactorialResponse(factor);
    }

    @Override
    protected void cleanup(final CoChannel channel){
        super.cleanup(channel);
        release(backendChans);
    }

    private void release(final PullCoChannel[] channels){
        if(channels == null){
            return;
        }

        for(final PullCoChannel chan: channels){
            if(chan == null){
                break;
            }
            IoUtils.close(chan);
        }
        backendChans = null;
    }

}
