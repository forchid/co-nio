package io.conio;

/**
 * <p>
 * The coroutine channel initializer, must set channel handler here.
 * </p>
 * @since 2018-08-09
 * @author little-pan
 */
public interface ChannelInitializer {

    ChannelInitializer NOOP = new ChannelInitializer(){
        @Override
        public void initialize(CoChannel channel, boolean serverSide){}
    };

    void initialize(CoChannel channel, boolean serverSide);

}
