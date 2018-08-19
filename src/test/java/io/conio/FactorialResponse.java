package io.conio;

import org.apache.commons.lang3.ObjectUtils;

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
