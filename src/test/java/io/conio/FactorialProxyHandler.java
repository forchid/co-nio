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
        final CoChannel channel = (CoChannel)co.getContext();
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
        int sizePerShard = range / backends.length;
        if(sizePerShard == 0){
            sizePerShard = 1;
        }
        final int shards = range / sizePerShard;
        System.out.println("Request: "+ request +", Shards: " + shards);
        final List<CoFuture<FactorialResponse>> rfutures = new ArrayList<>(shards);
        for(int from = request.from, i = 0; from <= request.to; ++i){
            final PullCoChannel chan = backendChans[i];
            final int a = from, to;
            if(i == shards - 1){
                final int rem = range % backends.length;
                to = sizePerShard + rem;
                from += sizePerShard + rem;
            }else {
                to = sizePerShard;
                from += sizePerShard;
            }
            final CoFuture<FactorialResponse> cf = chan.execute((c) -> {
                try{
                    final ByteBuffer buf = ByteBuffer.allocate(1024);
                    final FactorialRequest req = new FactorialRequest(a, to);
                    FactorialCodec.encodeRequest(c, buf, req);
                    return FactorialCodec.decodeResponse(c, buf);
                }catch (final IOException e){
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
            });
            rfutures.add(cf);
        }
        final FactorialResponse responses[] = new FactorialResponse[shards];
        for(int i = 0; i < shards; ++i){
            try{
                responses[i] = rfutures.get(i).get(co);
            }catch(final Throwable cause){
                log.warn("Calc error", cause);
                release(backendChans);
                String error = cause.getMessage();
                return new FactorialResponse(error);
            }
        }

        // 3. merge
        BigInteger factor = responses[0].factor;
        System.out.println(String.format("f%d: %s", 0, factor));
        for(int i = 1; i < responses.length; ++i){
            factor = factor.multiply(responses[i].factor);
            System.out.println(String.format("f%d: %s", i, factor));
        }
        return new FactorialResponse(factor);
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
