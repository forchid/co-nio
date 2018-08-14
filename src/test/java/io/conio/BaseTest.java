package io.conio;

public class BaseTest {

    // statistics
    protected long bytes = 0L, times = 0L;

    protected BaseTest(){

    }

    static void sleep(long millis){
        try {
            Thread.sleep(millis);
        }catch(InterruptedException e){}
    }

}
