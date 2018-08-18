package io.conio;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * The push model CoChannel that pushes the processing logic to the handler.
 * </p>
 * @since 2018-08-09
 * @author little-pan
 */
public abstract class PushCoChannel extends PushCoRunner implements CoChannel {
    final static Logger log = LoggerFactory.getLogger(PushCoChannel.class);

    protected PushCoChannel(final int id, CoGroup group){
        super(id, "pushChan-co-"+id, group);
    }

}
