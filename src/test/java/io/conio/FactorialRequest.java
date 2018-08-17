package io.conio;

public class FactorialRequest {

    public final int from;
    public final int to;

    public int size = 8;

    protected FactorialRequest(int from, int to){
        this.from = from;
        this.to   = to;
    }

    @Override
    public String toString(){
        return String.format("[%d, %d]", from, to);
    }

}
