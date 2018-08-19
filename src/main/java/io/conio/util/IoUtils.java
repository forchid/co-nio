package io.conio.util;

import java.io.Closeable;
import java.io.IOException;

public final class IoUtils {

    private IoUtils(){}

    public final static void close(Closeable closeable){
        if(closeable != null){
            try{
                closeable.close();
            }catch (IOException e){}
        }
    }

}
