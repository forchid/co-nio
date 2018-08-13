package io.conio;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import com.offbynull.coroutines.user.Continuation;
import com.offbynull.coroutines.user.Coroutine;
import com.offbynull.coroutines.user.CoroutineRunner;
import io.conio.util.CoFuture;
import io.conio.util.IoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  <p>
 *  The coroutine group that creates and schedules coroutines such as accept-co, channel-co etc.
 *  </p>
 * @since 2018-08-09
 * @author little-pan
 */
public class CoGroup {
    final static Logger log = LoggerFactory.getLogger(CoGroup.class);

    private String name = "coGroup";
    private boolean useAio;
    private boolean daemon;
    private String host;
    private int port = 9696;
    private int backlog = 1000;

    private volatile boolean stopped;
    private volatile boolean shutdown;
    private IoGroup ioGroup;

    private ChannelInitializer initializer = ChannelInitializer.NOOP;
    private int workerThreads = Runtime.getRuntime().availableProcessors();
    private ExecutorService workerThreadPool;

    protected CoGroup(){

    }

    public void start(){
        final String name = getName();
        final IoGroup group;
        if(isUseAio()){
            group = bootAio(name + "-aio");
        }else{
            group = bootNio(name + "-nio");
        }
        group.start();
        ioGroup = group;
    }

    public final boolean inGroup(){
        return ioGroup.inGroup();
    }

    public final boolean isStopped(){
        return stopped;
    }

    public void shutdown(){
        shutdown = true;
        ioGroup.shutdown();
    }

    public final boolean isShutdown(){
        return shutdown;
    }

    public void await(){
        final IoGroup group = ioGroup;
        if(group == null){
            return;
        }
        group.await();
    }

    public ChannelInitializer channelInitializer(){
        return initializer;
    }

    public void connect(String host, int port) {
        connect(new InetSocketAddress(host, port));
    }

    public void connect(String host, int port, CoHandler handler) {
        connect(new InetSocketAddress(host, port), handler);
    }

    public void connect(InetSocketAddress remote) {
        connect(remote, null);
    }

    public void connect(InetSocketAddress remote, CoHandler handler) {
        if(isStopped()){
            throw new IllegalStateException(name+" has stopped");
        }
        if(isShutdown()){
            throw new IllegalStateException(name+" has shutdown");
        }
        ioGroup.connect(new ConnectRequest(remote, handler));
    }

    public CoFuture<PullCoChannel> connect(CoChannel source, String host, int port) {
        return connect(source, new InetSocketAddress(host, port));
    }

    public CoFuture<PullCoChannel> connect(CoChannel source, InetSocketAddress remote) {
        if(isStopped()){
            throw new IllegalStateException(name+" has stopped");
        }
        if(isShutdown()){
            throw new IllegalStateException(name+" has shutdown");
        }
        if(!inGroup()){
            throw new IllegalStateException("The current coroutine not in this CoGroup " + name);
        }
        return ioGroup.connect(source, new ConnectRequest(remote, null));
    }

    protected IoGroup bootNio(String name){
        boolean failed;

        final Selector selector;
        Selector sel = null;
        failed = true;
        try{
            sel = Selector.open();
            selector = sel;
            failed = false;
        }catch (IOException e){
            throw new RuntimeException(e);
        }finally {
            if(failed){
                IoUtils.close(sel);
            }
        }

        final ServerSocketChannel serverChan;
        ServerSocketChannel chan = null;
        failed = true;
        try{
            final String host = getHost();
            if(host != null){
                chan = ServerSocketChannel.open();
                chan.configureBlocking(false);
                chan.bind(new InetSocketAddress(host, getPort()), getBacklog());
                chan.register(selector, SelectionKey.OP_ACCEPT);
            }
            serverChan = chan;
            failed = false;
        }catch (IOException e){
            throw new RuntimeException(e);
        }finally{
            if(failed){
                IoUtils.close(chan);
            }
        }

        return new NioGroup(this, name, serverChan, selector);
    }

    protected IoGroup bootAio(String name){
        boolean failed;

        final ExecutorService ioExec = Executors.newFixedThreadPool(1, (r) -> {
            final Thread t = new Thread(r, "aio-exec");
            t.setDaemon(isDaemon());
            return t;
        });
        final AsynchronousChannelGroup chanGroup;
        failed = true;
        try{
            chanGroup = AsynchronousChannelGroup.withThreadPool(ioExec);
            failed = false;
        }catch(IOException e){
            throw new RuntimeException(e);
        }finally {
            if(failed){
                ioExec.shutdown();
            }
        }

        final AsynchronousServerSocketChannel serverChan;
        AsynchronousServerSocketChannel chan = null;
        failed = true;
        try{
            final String host = getHost();
            if(host != null) {
                chan = AsynchronousServerSocketChannel.open(chanGroup);
                chan.bind(new InetSocketAddress(host, getPort()), getBacklog());
            }
            serverChan = chan;
            failed = false;
        }catch(IOException e){
            throw new RuntimeException(e);
        }finally {
            if(failed){
                IoUtils.close(chan);
                chanGroup.shutdown();
                ioExec.shutdown();
            }
        }

        return new AioGroup(this, name, serverChan, chanGroup);
    }

    static abstract class IoGroup implements Runnable {
        final static Logger log = LoggerFactory.getLogger(IoGroup.class);

        protected final BlockingQueue<CoTask> coQueue;

        protected final String name;
        protected final CoGroup coGroup;

        protected Thread runner;
        private int nextId;

        protected IoGroup(CoGroup coGroup, String name){
            this.coGroup = coGroup;
            this.name = name;
            this.coQueue = new LinkedTransferQueue<>();
        }

        @Override
        public abstract void run();

        public abstract CoFuture<PullCoChannel> connect(CoChannel source, ConnectRequest request);

        public abstract void connect(ConnectRequest request);

        public void start(){
            final Thread t = new Thread(this, name);
            t.setDaemon(coGroup.isDaemon());
            t.start();
            runner = t;
        }

        public void shutdown(){

        }

        public void await(){
            try {
                runner.join();
            }catch(InterruptedException e){}
        }

        public final boolean inGroup(){
            return (Thread.currentThread() == runner);
        }

        public int nextId(){
            return nextId++;
        }

        protected boolean offer(CoTask coTask){
           return coQueue.offer(coTask);
        }

        protected void cleanup(){
            coGroup.stopped = true;
            coGroup.ioGroup = null;
            coQueue.clear();
            coGroup.workerThreadPool.shutdown();
            log.info("Stopped");
        }

    }// IoGroup

    interface IoChannel {
        CoChannel coChannel();
        int read(Continuation co, ByteBuffer dst) throws IOException;
        int write(Continuation co, ByteBuffer src) throws IOException;
        boolean isOpen();
        void close();
    }// IoChannel

    interface CoTask extends Runnable {}

    boolean offer(CoTask coTask){
        return ioGroup.offer(coTask);
    }

    <V> CoFuture<V> execute(CoChannel chan, final Callable<V> callable){
        final CoGroup group = chan.group();
        if(group != this){
            throw new IllegalArgumentException("CoChannel group not this group");
        }
        final CompletableFuture<V> f = CompletableFuture.supplyAsync(()->{
            try {
                return callable.call();
            }catch(final Exception e){
                if(e instanceof RuntimeException){
                    throw (RuntimeException)e;
                }
                throw new RuntimeException(e);
            }
        }, group.workerThreadPool);
        final CoFutureImpl<V> cf = new CoFutureImpl<>(chan);
        f.handle((v, e) ->{
            if(e != null){
                cf.setCause(e);
            }else{
                cf.setValue(v);
            }
            offer(cf);
            return v;
        });
        return cf;
    }// execute()

    static class CoFutureImpl<V> implements CoFuture<V>, CoTask {
        final CoChannel chan;
        private boolean waited;

        private volatile boolean done;
        private V value;
        private Throwable cause;

        public CoFutureImpl(CoChannel chan){
            this.chan = chan;
        }

        @Override
        public V get(Continuation co) throws ExecutionException {
            for(;!isDone();){
                waited = true;
                co.suspend();
            }
            if(cause != null){
                if(cause instanceof  ExecutionException){
                    throw (ExecutionException)cause;
                }
                throw new ExecutionException(cause);
            }
            return value;
        }

        @Override
        public boolean isDone() {
            return done;
        }

        CoFutureImpl<V> setCause(Throwable cause){
            this.cause = cause;
            this.done = true;
            return this;
        }

        CoFutureImpl<V> setValue(V value){
            this.value = value;
            this.done = true;
            return this;
        }

        @Override
        public void run() {
            if(waited){
                chan.resume();
            }
        }
    }// CoFutureImpl

    static class NioGroup extends  IoGroup {
        final static Logger log = LoggerFactory.getLogger(NioGroup.class);

        final ServerSocketChannel serverChan;
        final Selector selector;

        public NioGroup(CoGroup coGroup, String name, ServerSocketChannel serverChan, Selector selector){
            super(coGroup, name);
            this.serverChan = serverChan;
            this.selector = selector;
        }

        @Override
        public void run(){
            try{
                if(coGroup.host != null){
                    log.info("Started on {}:{}", coGroup.host, coGroup.port);
                }else{
                    log.info("Started");
                }

                for(;!coGroup.isStopped();){
                    // 1. result handlers
                    handleCoTasks();

                    // 2. select events
                    final int n = selector.select(1000L);
                    if(n > 0){
                        final Set<SelectionKey> keys = selector.selectedKeys();
                        final Iterator<SelectionKey> i = keys.iterator();
                        for(; i.hasNext();i.remove()){
                            final SelectionKey key = i.next();
                            if(!key.isValid()){
                                continue;
                            }
                            if(key.isAcceptable()){
                                handleAcception(key);
                                continue;
                            }
                            if(key.isConnectable()){
                                handleConnection(key);
                                continue;
                            }
                            if(key.isReadable()){
                                handleRead(key);
                            }
                            if(key.isValid() && key.isWritable()){
                                handleWrite(key);
                            }
                        }// key-loop
                    }

                    // 3. handle shutdown
                    if(coGroup.isShutdown()){
                        log.debug("{}: shutdown", name);
                        // 3.1 not accept any new connection
                        IoUtils.close(serverChan);
                        // 3.2 check other connections closed
                        final Set<SelectionKey> keys = selector.keys();
                        final int keySize = keys.size();
                        if(keySize == 0){
                            break;
                        }
                        int closes = 0;
                        for(Iterator<SelectionKey> i = keys.iterator();i.hasNext();){
                            final SelectableChannel chan = i.next().channel();
                            if(!chan.isOpen()){
                                ++closes;
                            }
                        }
                        if(closes == keySize){
                            break;
                        }
                        log.debug("{}: selection keys {}, closed channels {}", name, keySize, closes);
                    }
                }// event-loop

            }catch(final IOException e){
                log.error("Nio group fatal error", e);
            }finally {
                cleanup();
            }
        }

        @Override
        protected boolean offer(CoTask handler){
            final boolean succ = super.offer(handler);
            if(succ){
                selector.wakeup();
            }
            return succ;
        }

        private void handleAcception(final SelectionKey key) throws IOException {
            final ServerSocketChannel serverChan = (ServerSocketChannel)key.channel();
            final SocketChannel chan = serverChan.accept();
            if(chan == null){
                return;
            }

            boolean failed = true;
            PushCoChannel coChan = null;
            try{
                chan.configureBlocking(false);
                final SelectionKey selKey = chan.register(selector, SelectionKey.OP_READ);
                coChan = new NioPushCoChannel(nextId(), this, chan, selKey);
                coGroup.channelInitializer().initialize(coChan, true);
                if(coChan.handler() == null){
                    log.warn("{}: Channel handler not set, so close the channel {}", name, coChan.name);
                    return;
                }
                log.debug("{}: start a new coChannel {}", name, coChan.name);
                coChan.resume();
                failed = false;
            }catch (final Throwable cause){
                if(coChan == null || coChan.handler() == null){
                    log.warn(name+": uncaught exception", cause);
                }else{
                    coChan.handler().uncaught(cause);
                }
                return;
            }finally {
                if(failed){
                    IoUtils.close(chan);
                }
            }
        }

        protected void handleConnection(final SelectionKey key){
            final NioConnectHandler handler = (NioConnectHandler)key.attachment();
            key.attach(null);

            final NioChannel ioChan = (NioChannel)handler.ioChan;
            final CoChannel coChan = ioChan.coChannel();
            final SocketChannel chan = ioChan.chan;
            boolean failed = true;
            try{
                chan.finishConnect();
                log.debug("{}: start a new coChannel {}", name, coChan.name);
                coChan.resume();
                handler.run();
                failed = false;
            }catch(final Throwable cause){
                handler.setCause(cause);
                handler.run();
                return;
            }finally {
                if(failed){
                    IoUtils.close(coChan);
                }
            }
        }

        protected void handleRead(final SelectionKey key) {
            final IoChannel coChan = (IoChannel) key.attachment();
            coChan.coChannel().resume();
        }

        protected void handleWrite(final SelectionKey key) {
            final IoChannel coChan = (IoChannel) key.attachment();
            coChan.coChannel().resume();
        }

        private void handleCoTasks(){
            for(;;){
                final CoTask coTask = coQueue.poll();
                if(coTask == null){
                    break;
                }
                coTask.run();
            }
        }

        @Override
        public void shutdown(){
            selector.wakeup();
        }

        @Override
        protected void cleanup(){
            IoUtils.close(serverChan);
            IoUtils.close(selector);
            super.cleanup();
        }

        @Override
        public CoFuture<PullCoChannel> connect(CoChannel source, ConnectRequest request) {
            return new NioConnectHandler(this, request).connect(source);
        }

        @Override
        public void connect(ConnectRequest request){
            new NioConnectHandler(this, request).connect();
        }

        static class NioConnectHandler implements CoTask {
            final static int STEP_INIT = 0;
            final static int STEP_CONN = 1;
            final static int STEP_COMP = 2;

            private int step = STEP_INIT;

            private final NioGroup ioGroup;
            private final ConnectRequest request;

            IoChannel ioChan;
            private CoFutureImpl<PullCoChannel> future;
            private Throwable cause;

            public NioConnectHandler(NioGroup ioGroup, ConnectRequest request){
                this.ioGroup = ioGroup;
                this.request = request;
            }

            public CoFuture<PullCoChannel> connect(CoChannel source) {
                connect();
                return (this.future = new CoFutureImpl<>(source));
            }

            public void connect(){
                ioGroup.offer(this);
            }

            public void setCause(Throwable cause){
                this.cause = cause;
            }

            @Override
            public void run() {
                final boolean pull = (future != null);
                switch(step){
                    case STEP_INIT:
                        CoHandler handler = request.handler;
                        boolean failed = true;
                        SocketChannel chan = null;
                        try{
                            final CoGroup coGroup = ioGroup.coGroup;
                            final ChannelInitializer chanInit = coGroup.channelInitializer();
                            chan = SocketChannel.open();
                            if(pull){
                                NioPullCoChannel pullChan = new NioPullCoChannel(ioGroup.nextId(), ioGroup, chan);
                                chanInit.initialize(pullChan, false);
                                this.ioChan = pullChan.ioChan;
                            }else{
                                NioPushCoChannel pushChan = new NioPushCoChannel(ioGroup.nextId(), ioGroup, chan);
                                chanInit.initialize(pushChan, false);
                                if(handler == null && (handler=pushChan.handler()) == null){
                                    log.warn("{}: Channel handler not set, so close channel {}",
                                            ioGroup.name, pushChan.name);
                                    return;
                                }
                                pushChan.handler(handler);
                                this.ioChan = pushChan.ioChan;
                            }
                            log.debug("{}: connect to {}", ioGroup.name, request.remote);
                            chan.configureBlocking(false);
                            chan.register(ioGroup.selector, SelectionKey.OP_CONNECT, this);
                            chan.connect(request.remote);
                            this.step = STEP_CONN;
                            failed = false;
                        }catch(final Throwable cause){
                            if(pull){
                                future.setCause(cause).run();
                                return;
                            }
                            if(handler == null){
                                log.warn(ioGroup.name+": Channel handler not set, uncaught exception", cause);
                            }else{
                                handler.uncaught(cause);
                            }
                        }finally {
                            if(failed){
                                step = STEP_COMP;
                                IoUtils.close(chan);
                            }
                        }
                        break;
                    case STEP_CONN:
                        try{
                            if(pull){
                                if(cause != null){
                                    future.setCause(cause);
                                }else{
                                    final PullCoChannel coChan = (NioPullCoChannel)ioChan.coChannel();
                                    future.setValue(coChan);
                                }
                                future.run();
                                return;
                            }

                            final PushCoChannel coChan = (NioPushCoChannel)ioChan.coChannel();
                            if(cause != null){
                                coChan.handler().uncaught(cause);
                                return;
                            }
                            coChan.resume();
                        }finally {
                            step = STEP_COMP;
                        }
                        break;
                    case STEP_COMP:
                    default:
                        log.warn("{}: connect has completed", ioGroup.name);
                         break;
                }
            }
        }

        static class NioChannel implements IoChannel {
            final CoChannel coChan;
            final NioGroup ioGroup;
            final SocketChannel chan;
            private SelectionKey selKey;

            public NioChannel(CoChannel coChan, NioGroup ioGroup, SocketChannel chan){
                this(coChan, ioGroup, chan, null);
            }

            public NioChannel(CoChannel coChan, NioGroup ioGroup, SocketChannel chan, SelectionKey selKey){
                this.coChan = coChan;
                this.ioGroup = ioGroup;
                this.chan = chan;
                this.selKey = selKey;
                if(selKey != null){
                    selKey.attach(this);
                }
            }

            @Override
            public CoChannel coChannel(){
                return coChan;
            }

            @Override
            public int read(Continuation co, ByteBuffer dst) throws IOException {
                if(!dst.hasRemaining()){
                    return 0;
                }
                int n = 0;
                boolean readable = false;
                try{
                    for(;dst.hasRemaining();){
                        final int i = chan.read(dst);
                        if(i == -1){
                            if(n == 0){
                                return -1;
                            }
                            break;
                        }
                        if(i == 0){
                            if(n > 0){
                                return n;
                            }
                            enableRead();
                            readable = true;
                            co.suspend();
                            continue;
                        }
                        n += i;
                    }
                    return n;
                }finally {
                    if(readable){
                        disableRead();
                    }
                }
            }

            @Override
            public int write(Continuation co, ByteBuffer src) throws IOException {
                if(!src.hasRemaining()){
                    return 0;
                }
                int n = 0;
                try{
                    enableWrite(); // must first enable write?
                    co.suspend();
                    for(;src.hasRemaining();){
                        final int i = chan.write(src);
                        if(i == 0){
                            enableWrite();
                            co.suspend();
                            continue;
                        }
                        n += i;
                    }
                    return n;
                }finally {
                    disableWrite();
                }
            }

            @Override
            public boolean isOpen() {
                return chan.isOpen();
            }

            @Override
            public void close() {
                IoUtils.close(chan);
                if(log.isDebugEnabled()) {
                    final CoGroup group = ioGroup.coGroup;
                    log.debug("{}: {} closed", group.name, coChan.name);
                }
            }

            protected void enableRead()throws IOException {
                final int op = SelectionKey.OP_READ;
                if(selKey == null){
                    selKey = chan.register(ioGroup.selector, op, this);
                }else{
                    final int ops = selKey.interestOps();
                    if((ops & op) == 0){
                        selKey.interestOps(ops | op);
                    }
                }
            }

            protected final void disableRead(){
                final int op = SelectionKey.OP_READ;
                if(selKey != null){
                    final int ops = selKey.interestOps();
                    if((ops & op) != 0){
                        selKey.interestOps(ops & ~op);
                    }
                }
            }

            protected final void enableWrite()throws IOException {
                final int op = SelectionKey.OP_WRITE;
                if(selKey == null){
                    selKey = chan.register(ioGroup.selector, op, this);
                }else{
                    final int ops = selKey.interestOps();
                    if((ops & op) == 0){
                        selKey.interestOps(ops | op);
                    }
                }
            }

            protected final void disableWrite() {
                final int op = SelectionKey.OP_WRITE;
                if(selKey != null){
                    final int ops = selKey.interestOps();
                    if((ops & op) != 0){
                        selKey.interestOps(ops & ~op);
                    }
                }
            }

        }// NioChannel

        static class NioPushCoChannel extends PushCoChannel {
            final NioChannel ioChan;

            public NioPushCoChannel(final int id, NioGroup ioGroup, SocketChannel chan){
                this(id, ioGroup, chan, null);
            }

            public NioPushCoChannel(final int id, NioGroup ioGroup, SocketChannel chan, SelectionKey selKey){
                super(id, ioGroup.coGroup);
                this.ioChan = new NioChannel(this, ioGroup, chan, selKey);
            }

            @Override
            public int read(Continuation co, ByteBuffer dst) throws IOException {
                return ioChan.read(co, dst);
            }

            @Override
            public int write(Continuation co, ByteBuffer src) throws IOException {
                return ioChan.write(co, src);
            }

            @Override
            public boolean isOpen() {
                return ioChan.isOpen();
            }

            @Override
            public void close() {
                ioChan.close();
            }
        }// NioPushCoChannel

        static class NioPullCoChannel extends PullCoChannel {
            final NioChannel ioChan;

            public NioPullCoChannel(final int id, NioGroup ioGroup, SocketChannel chan){
                this(id, ioGroup, chan, null);
            }

            public NioPullCoChannel(final int id, NioGroup ioGroup, SocketChannel chan, SelectionKey selKey){
                super(id, ioGroup.coGroup);
                this.ioChan = new NioChannel(this, ioGroup, chan, selKey);
            }

            @Override
            public int read(Continuation co, ByteBuffer dst) throws IOException {
                return ioChan.read(co, dst);
            }

            @Override
            public int write(Continuation co, ByteBuffer src) throws IOException {
                return ioChan.write(co, src);
            }

            @Override
            public boolean isOpen() {
                return ioChan.isOpen();
            }

            @Override
            public void close() {
                ioChan.close();
            }
        }// NioPullCoChannel

    }// NioGroup

    static class AioGroup extends IoGroup {
        final static Logger log = LoggerFactory.getLogger(AioGroup.class);

        final AsynchronousServerSocketChannel serverChan;
        final AsynchronousChannelGroup chanGroup;
        AioCoAcceptor coAcceptor = null;

        public AioGroup(CoGroup coGroup, String name, AsynchronousServerSocketChannel serverChan,
                        AsynchronousChannelGroup chanGroup){
            super(coGroup, name);
            this.serverChan = serverChan;
            this.chanGroup  = chanGroup;
        }

        @Override
        public void run() {
            try{
                if(serverChan != null){
                    coAcceptor = new AioCoAcceptor(this, serverChan);
                    coAcceptor.start();
                    log.info("Started on {}:{}", coGroup.host, coGroup.port);
                }else{
                    log.info("Started");
                }

                for (;!coGroup.isStopped();){
                    final CoTask handler = coQueue.poll(1L, TimeUnit.SECONDS);
                    if(handler != null){
                        handler.run();
                    }
                    if(coGroup.isShutdown()){
                        for(;;){
                            final CoTask h = coQueue.poll();
                            if(h == null){
                                break;
                            }
                            h.run();
                        }
                        break;
                    }
                } // poll-loop

            }catch(InterruptedException e){
                log.warn("Exit: interrupted");
            }finally{
                cleanup();
            }
        }

        @Override
        protected void cleanup(){
            if(coAcceptor != null){
                coAcceptor.stop();
            }
            IoUtils.close(serverChan);
            super.cleanup();
        }

        @Override
        public CoFuture<PullCoChannel> connect(final CoChannel source, ConnectRequest request) {
            final AioConnectHandler handler = new AioConnectHandler(this, request);
            return handler.connect(source);
        }

        @Override
        public void connect(final ConnectRequest request) {
            final AioConnectHandler handler = new AioConnectHandler(this, request);
            handler.connect();
        }

        static class AioConnectHandler implements CoTask, CompletionHandler<Void, ConnectRequest> {
            private CoFutureImpl<PullCoChannel> future;

            final AioGroup aioGroup;
            final ConnectRequest request;

            AsynchronousSocketChannel channel;
            Throwable cause;

            public AioConnectHandler(AioGroup aioGroup, ConnectRequest request){
                this.aioGroup= aioGroup;
                this.request = request;
            }

            private void openChannel() {
                AsynchronousSocketChannel chan = null;
                boolean failed = true;
                try {
                    chan = AsynchronousSocketChannel.open(aioGroup.chanGroup);
                    channel = chan;
                    failed = false;
                }catch(final IOException e){
                    throw new RuntimeException(e);
                }finally {
                    if(failed){
                        IoUtils.close(chan);
                    }
                }
            }

            public void connect(){
                openChannel();
                log.debug("{}: connect to {}", aioGroup.name, request.remote);
                channel.connect(request.remote, request, this);
            }

            public CoFuture<PullCoChannel> connect(final CoChannel source) {
                connect();
                return (future = new CoFutureImpl<>(source));
            }

            @Override
            public void run() {
                final boolean pull = (future != null);
                boolean failed = true;
                log.debug("{}: Handle connection result", aioGroup.name);

                CoChannel coChan  = null;
                CoHandler handler = request.handler;
                try {
                    final CoGroup coGroup = aioGroup.coGroup;
                    final ChannelInitializer chanInit = coGroup.channelInitializer();
                    if (pull) {
                        if (cause != null) {
                            future.setCause(cause);
                            return;
                        }
                        final PullCoChannel pullChan = new AioPullCoChannel(aioGroup, channel);
                        coChan = pullChan;
                        chanInit.initialize(pullChan, false);
                        future.setValue(pullChan);
                    } else {
                        final PushCoChannel pushChan = new AioPushCoChannel(aioGroup, channel);
                        coChan = pushChan;
                        chanInit.initialize(pushChan, false);
                        if (handler == null && (handler = pushChan.handler()) == null) {
                            log.warn("{}: Connect handler not set, so close the channel", aioGroup.name);
                            return;
                        }
                        pushChan.handler(handler);
                        if (cause != null) {
                            handler.uncaught(cause);
                            return;
                        }
                    }
                    log.debug("{}: start a new CoChannel {}", aioGroup.name, coChan.name);
                    coChan.resume();
                    failed = false;
                }catch (final Throwable e){
                    if(pull){
                        future.setCause(e);
                        return;
                    }
                    if(handler == null){
                        log.warn(aioGroup.name + ": Channel handler not set, uncaught exception", e);
                    }else {
                        handler.uncaught(e);
                    }
                }finally {
                    if(failed){
                        IoUtils.close(coChan);
                        IoUtils.close(channel);
                    }
                    if(pull){
                        future.run();
                    }
                }
            }

            @Override
            public void completed(Void none, ConnectRequest request) {
                final AioConnectHandler handler = new AioConnectHandler(aioGroup, request);
                handler.channel = channel;
                handler.future  = future;
                aioGroup.offer(handler);
            }

            @Override
            public void failed(Throwable cause, ConnectRequest request) {
                final AioConnectHandler handler = new AioConnectHandler(aioGroup, request);
                handler.cause  = cause;
                handler.future = future;
                aioGroup.offer(handler);
            }
        }

        static class AioCoAcceptor implements Coroutine {
            final static Logger log = LoggerFactory.getLogger(AioCoAcceptor.class);

            final String name = "aioAccept-co";

            final CoroutineRunner runner = new CoroutineRunner(this);
            final AcceptHandler handler = new AcceptHandler();

            final AsynchronousServerSocketChannel chan;
            final AioGroup aioGroup;
            final CoGroup coGroup;

            boolean stopped;

            public AioCoAcceptor(final AioGroup aioGroup, AsynchronousServerSocketChannel chan){
                this.aioGroup = aioGroup;
                this.coGroup  = aioGroup.coGroup;
                this.chan = chan;
            }

            @Override
            public void run(Continuation co){
                log.info("{}: started", name);
                for (;!coGroup.isShutdown() && !stopped;){
                    chan.accept(this, handler);
                    co.suspend();
                }
                log.info("{}: stopped", name);
            }

            public boolean resume(){
                return runner.execute();
            }

            public boolean start(){
                return resume();
            }

            public boolean stop(){
                stopped = true;
                return resume();
            }

            class AcceptResultHandler implements CoTask {
                final AsynchronousSocketChannel chan;
                final Throwable cause;

                public AcceptResultHandler(AsynchronousSocketChannel chan){
                    this(chan, null);
                }

                public AcceptResultHandler(Throwable cause){
                    this(null, cause);
                }

                private AcceptResultHandler(AsynchronousSocketChannel chan, Throwable cause){
                    this.chan = chan;
                    this.cause= cause;
                }

                @Override
                public void run(){
                    final AioCoAcceptor acceptor = AioCoAcceptor.this;
                    if(cause != null){
                        acceptor.stop();
                        log.warn(name+" error", cause);
                        return;
                    }
                    // Start a coroutine for handle socket channel
                    boolean failed = true;
                    try{
                        final AioGroup aioGroup = acceptor.aioGroup;
                        final AioPushCoChannel coChan = new AioPushCoChannel(aioGroup, chan);
                        log.debug("{}: accept a new coChannel {}", name, coChan.name);
                        coGroup.channelInitializer().initialize(coChan, true);
                        if(coChan.handler() == null){
                            log.warn("{}: Channel handler not set, so close the channel");
                            IoUtils.close(chan);
                            return;
                        }
                        coChan.resume();
                        failed = false;
                    }finally{
                        acceptor.resume();
                        if(failed){
                            IoUtils.close(chan);
                        }
                    }
                }
            }

            class AcceptHandler implements CompletionHandler<AsynchronousSocketChannel, AioCoAcceptor> {

                @Override
                public void completed(AsynchronousSocketChannel result, AioCoAcceptor acceptor){
                    aioGroup.offer(new AcceptResultHandler(result));
                }

                @Override
                public void failed(Throwable cause, AioCoAcceptor acceptor){
                    aioGroup.offer(new AcceptResultHandler(cause));
                }

            }

        }// CoAcceptor

        static class AioPushCoChannel extends PushCoChannel {
            final AioChannel ioChan;

            public AioPushCoChannel(AioGroup aioGroup, AsynchronousSocketChannel chan){
                super(aioGroup.nextId(), aioGroup.coGroup);
                this.ioChan = new AioChannel(this, aioGroup, chan);
            }

            @Override
            public int read(Continuation co, ByteBuffer dst) throws IOException {
                return ioChan.read(co, dst);
            }

            @Override
            public int write(Continuation co, ByteBuffer src) throws IOException {
                return ioChan.write(co, src);
            }

            @Override
            public boolean isOpen() {
                return ioChan.isOpen();
            }

            @Override
            public void close() {
                ioChan.close();
            }
        }// AioPushCoChannel

        static class AioPullCoChannel extends PullCoChannel {
            final AioChannel ioChan;

            public AioPullCoChannel(AioGroup aioGroup, AsynchronousSocketChannel chan){
                super(aioGroup.nextId(), aioGroup.coGroup);
                this.ioChan = new AioChannel(this, aioGroup, chan);
            }

            @Override
            public int read(Continuation co, ByteBuffer dst) throws IOException {
                return ioChan.read(co, dst);
            }

            @Override
            public int write(Continuation co, ByteBuffer src) throws IOException {
                return ioChan.write(co, src);
            }

            @Override
            public boolean isOpen() {
                return ioChan.isOpen();
            }

            @Override
            public void close() {
                ioChan.close();
            }
        }// AioPullCoChannel

        static class AioChannel implements IoChannel {
            final CoChannel coChan;

            final AioGroup aioGroup;
            final AsynchronousSocketChannel chan;
            final IoHandler handler = new IoHandler();
            private IoResultHandler result;

            public AioChannel(CoChannel coChan, AioGroup aioGroup, AsynchronousSocketChannel chan){
                this.coChan = coChan;
                this.aioGroup = aioGroup;
                this.chan = chan;
            }

            @Override
            public CoChannel coChannel(){
                return coChan;
            }

            @Override
            public int read(Continuation co, ByteBuffer dst) throws IOException {
                if(!dst.hasRemaining()){
                    return 0;
                }
                try{
                    chan.read(dst, null, handler);
                    co.suspend();
                    if(result.cause != null){
                        throw new IOException(result.cause);
                    }
                    return result.bytes;
                } finally {
                    result = null;
                }
            }

            @Override
            public int write(Continuation co, ByteBuffer src) throws IOException {
                if(!src.hasRemaining()){
                    return 0;
                }
                try{
                    int n = 0;
                    for(;src.hasRemaining();){
                        chan.write(src, null, handler);
                        co.suspend();
                        if(result.cause != null){
                            throw new IOException(result.cause);
                        }
                        n += result.bytes;
                    }
                    return n;
                } finally {
                    result = null;
                }
            }

            @Override
            public boolean isOpen() {
                return chan.isOpen();
            }

            @Override
            public void close() {
                IoUtils.close(chan);
                if(log.isDebugEnabled()){
                    final CoGroup group = aioGroup.coGroup;
                    log.debug("{}: {} closed", group.name, coChan.name);
                }
            }

            // Running in io threads.
            class IoHandler implements CompletionHandler<Integer, Void> {

                @Override
                public void completed(Integer result, Void attachment) {
                    aioGroup.offer(new IoResultHandler(result));
                }

                @Override
                public void failed(Throwable cause, Void attachment) {
                    aioGroup.offer(new IoResultHandler(cause));
                }
            }// IoHandler

            class IoResultHandler implements CoTask {

                final Integer bytes;
                final Throwable cause;

                IoResultHandler(Integer bytes){
                    this(bytes, null);
                }

                IoResultHandler(Throwable cause){
                    this(null, cause);
                }

                IoResultHandler(Integer bytes, Throwable cause){
                    this.bytes = bytes;
                    this.cause = cause;
                }

                @Override
                public void run() {
                    result = this;
                    coChannel().resume();
                }
            }// IoResultHandler

        }// AioCoChannel

    }// AioGroup

    public String getName(){
        return name;
    }

    public boolean isUseAio(){
        return useAio;
    }

    public boolean isDaemon(){
        return daemon;
    }

    public String getHost(){
        return host;
    }

    public int getPort(){
        return port;
    }

    public int getBacklog(){
        return backlog;
    }

    public int getWorkerThreads(){
        return workerThreads;
    }

    public final static Builder newBuilder(){
        return new Builder();
    }

    public static class Builder {
        private CoGroup group;

        protected Builder(){
            this.group = new CoGroup();
        }

        public Builder setName(String name){
            group.name = name;
            return this;
        }

        public Builder useAio(boolean use){
            group.useAio = use;
            return this;
        }

        public Builder setDaemon(boolean daemon){
            group.daemon = daemon;
            return this;
        }

        public Builder setHost(String host){
            group.host = host;
            return this;
        }

        public Builder setPort(int port){
            group.port = port;
            return this;
        }

        public Builder setBacklog(int backlog){
            group.backlog = backlog;
            return this;
        }

        public Builder setworkerThreads(int workerThreads){
            group.workerThreads = workerThreads;
            return this;
        }

        public Builder channelInitializer(ChannelInitializer initializer){
            group.initializer = initializer;
            return this;
        }

        public CoGroup build(){
            if(group.channelInitializer() == null){
                throw new IllegalStateException("Channel initializer not set");
            }
            final int workerThreads = group.getWorkerThreads();
            if(workerThreads < 1){
                throw new IllegalArgumentException("workerThreads smaller than 1: " + workerThreads);
            }
            group.workerThreadPool = Executors.newFixedThreadPool(workerThreads, new ThreadFactory() {
                final AtomicInteger counter = new AtomicInteger(0);
                @Override
                public Thread newThread(Runnable r) {
                    final String name = String.format("%s-worker-%d", group.getName(), counter.incrementAndGet());
                    final Thread t = new Thread(r, name);
                    t.setDaemon(group.isDaemon());
                    return t;
                }
            });
            return group;
        }

    }// Builder

    static class ConnectRequest {
        final InetSocketAddress remote;
        final CoHandler handler;

        public ConnectRequest(InetSocketAddress remote, CoHandler handler){
            this.remote = remote;
            this.handler= handler;
        }
    }

}
