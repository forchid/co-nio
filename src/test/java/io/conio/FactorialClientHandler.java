package io.conio;

import com.offbynull.coroutines.user.Continuation;
import io.conio.util.IoUtils;

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

    public FactorialClientHandler(){

    }

    @Override
    public void handle(Continuation co) {
        final CoChannel channel = (CoChannel)co.getContext();
        int i = 1;
        try{
            final CoGroup group = channel.group();
            for(;!group.isShutdown(); ++i){
                BigInteger result = calc(co, 1, i);
                if(i <= FACTORS.length && result.compareTo(FACTORS[i - 1]) != 0){
                    log.warn("Result error: {} expect {}", result, FACTORS[i - 1]);
                    break;
                }
                ++times;
            }
        }catch (final IOException cause){
            log.warn("IO error", cause);
        }finally {
            IoUtils.close(channel);
        }
    }

    private BigInteger calc(Continuation co, final int from, final int to)throws IOException {
        final FactorialRequest request = new FactorialRequest(from, to);
        bytes += FactorialCodec.encodeRequest(co, buffer, request);

        final FactorialResponse response = FactorialCodec.decodeResponse(co, buffer);
        bytes += response.size;
        if(response.error != null){
            throw new IOException(response.error);
        }
        return response.factor;
    }

}
