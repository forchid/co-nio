package io.conio;

/**
 * <p>
 * The pull mode CoChannel.
 * </p>
 * @since 2018-08-09
 * @author little-pan
 */
public abstract class PullCoChannel extends PullCoRunner implements CoChannel {

    protected PullCoChannel(final int id, CoGroup group){
        super(id, "pullChan-co-"+id, group);
    }

    protected PullCoChannel(PullCoChannel wrapped){
        super(wrapped);
    }

}
