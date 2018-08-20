/*
 * Copyright (c) 2018, little-pan, All rights reserved.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3.0 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library.
 */
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
