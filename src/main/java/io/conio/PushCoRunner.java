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

import com.offbynull.coroutines.user.Continuation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * The push model CoRunner that pushes the processing logic to the handler.
 * </p>
 * @autor little-pan
 * @since 2018-08-18
 */
public class PushCoRunner extends CoRunner {
    final static Logger log = LoggerFactory.getLogger(PushCoRunner.class);

    protected CoHandler handler;

    public PushCoRunner(int id, CoGroup group){
        this(id, "push-co-"+id, group);
    }

    public PushCoRunner(int id, String name, CoGroup group){
        super(id, name, group);
    }

    @Override
    public void run(Continuation co){
        log.debug("{}: Started", name);
        co.setContext(this);
        handler.handle(co);
        log.debug("{}: Stopped", name);
    }

    public CoHandler handler(){
        return handler;
    }

    public PushCoRunner handler(CoHandler handler){
        this.handler = handler;
        return this;
    }

}
