package io.conio;

import java.math.BigInteger;

public class FactorialResponse {

    public final BigInteger factor;
    public final String error;

    public int size;

    public FactorialResponse(BigInteger factor){
        this(factor, null);
    }

    public FactorialResponse(String error){
        this(null, error);
    }

    protected FactorialResponse(BigInteger factor, String error){
        this.factor = factor;
        this.error  = error;
    }

}
