# co-nio
A simple nio framework based on coroutines

## co-nio features
1. Nio and aio group based coroutine scheduler.
2. Push and pull mode coroutine, coroutine channel.
3. Coroutine future for supporting async programming style.
4. Computation task execution in a worker thread pool.
5. Using offbynull's high performance [coroutines](https://github.com/offbynull/coroutines) library.
6. Coroutine and coroutine channel timer.
7. Pull coroutine channel connection pool.

### a sample
First we boot the server,
```
final CoGroup serverGroup = CoGroup.newBuilder()
    .setHost(HOST)
    .setName("serverCoGroup")
    .channelInitializer((channel, sside) -> {
        if(sside) {
            final PushCoChannel chan = (PushCoChannel)channel;
            chan.handler(new EchoServerHandler());
         }
     })
    .build();
     serverGroup.start();
```
The server handler,
```
public class EchoServerHandler implements CoHandler {
    final static Logger log = LoggerFactory.getLogger(EchoServerHandler.class);

    final ByteBuffer buffer;

    public EchoServerHandler(){
        this(8192);
    }

    public EchoServerHandler(final int bufferSize){
        buffer = ByteBuffer.allocate(bufferSize);
    }

    @Override
    public void handle(Continuation co) {
        final PushCoChannel channel = (PushCoChannel)co.getContext();
        try{
            for(;!channel.group().isShutdown();){
                final int n = channel.read(co, buffer);
                if(n == -1){
                    break;
                }
                //log.debug("{}: recv {} bytes", channel.name, n);
                buffer.flip();
                for(;buffer.hasRemaining();) {
                    final int i = channel.write(co, buffer);
                    //log.debug("{}: send {} bytes", channel.name, i);
                }
                buffer.clear();
            }
        }catch(final IOException e){
            log.warn("IO error", e);
        }finally {
            IoUtils.close(channel);
        }
    }

}
```
Then boot the client,
```
final CoGroup clientGroup = CoGroup.newBuilder()
    .setName("clientCoGroup")
    .build();
    clientGroup.start();

    final int n = 10;
    final EchoClientHandler handlers[] = new EchoClientHandler[n];
    for(int i = 0; i < n; ++i){
        final EchoClientHandler handler = new EchoClientHandler(1024);
        clientGroup.connect(HOST, PORT, handler);
        handlers[i] = handler;
    }
    sleep(30 * 1000L);

    clientGroup.shutdown();
    clientGroup.await();
```
The client handler,
```
public class EchoClientHandler extends BaseTest implements CoHandler {
    final static Logger log = LoggerFactory.getLogger(EchoClientHandler.class);

    final ByteBuffer buffer;
    final byte[] data;

    public EchoClientHandler(){
        this(8192);
    }

    public EchoClientHandler(final int bufferSize){
        buffer = ByteBuffer.allocate(bufferSize);
        data   = new byte[bufferSize];
        for(int i = 0, size = data.length; i < size; ++i){
            data[i] = (byte)i;
        }
    }

    @Override
    public void handle(Continuation co) {
        final PushCoChannel channel = (PushCoChannel)co.getContext();
        try{
            final ByteBuffer dbuf = ByteBuffer.wrap(data);
            final CoGroup group = channel.group();
            for(;!group.isShutdown();){
                for(;dbuf.hasRemaining();){
                    final int n = channel.write(co, dbuf);
                    bytes += n;
                    //log.debug("{}: send {} bytes", channel.name, n);
                }
                dbuf.flip();
                for(;buffer.hasRemaining();){
                    final int n = channel.read(co, buffer);
                    if(n == -1){
                        throw new EOFException("Server closed");
                    }
                    bytes += n;
                }
                buffer.flip();
                if(!Arrays.equals(data, buffer.array())){
                    throw new IOException("Packet malformed");
                }
                buffer.clear();
                ++times;
            }
        }catch(final IOException e){
            log.warn("IO error", e);
        }finally {
            IoUtils.close(channel);
        }
    }

}
```
