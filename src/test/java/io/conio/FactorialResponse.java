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

import java.math.BigInteger;

public class FactorialResponse {

    public final BigInteger factor;
    public final String error;

    public int size;

    public FactorialResponse(BigInteger factor){
        this(factor, null);
        if(factor == null){
            throw new NullPointerException("factor null");
        }
    }

    public FactorialResponse(String error){
        this(null, error);
        if(error == null){
            throw new NullPointerException("error null");
        }
    }

    protected FactorialResponse(BigInteger factor, String error){
        this.factor = factor;
        this.error  = error;
    }

    @Override
    public String toString(){
        return String.format("factorial: %s, error: %s", factor, error);
    }

}
