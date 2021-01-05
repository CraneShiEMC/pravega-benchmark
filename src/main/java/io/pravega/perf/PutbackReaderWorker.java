package io.pravega.perf;

import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.stream.*;
import io.pravega.client.stream.impl.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class PutbackReaderWorker extends ReaderWorker {
    private static Logger log = LoggerFactory.getLogger(PravegaReaderWorker.class);
    private final Random r = new Random();
    private final int events;
    private long consumeTime = 500;
    private static final int EVENT_LOSS_THRESHOLD = 3600 * 1000;
    private long consumeTimeVariance = 200;
    private final List<EventStreamReader<byte[]>> readers = new ArrayList<>();
    private final Stream stream;
    private final EventStreamWriter<byte[]> producer;
    private final ExecutorService executorService = Executors.newCachedThreadPool();
    private final ExecutorService readerExecutorService;
    private final ScheduledExecutorService validatorService = Executors.newSingleThreadScheduledExecutor();
    private final List<Future<?>> futures = new ArrayList<>();
    private final Set<String> writtenEvents = ConcurrentHashMap.newKeySet();
    private final AtomicLong currentEpoch = new AtomicLong(0);
    private final Object lock = new Object();

    PutbackReaderWorker(int consumerCount, int events, int secondsToRun, long start, PerfStats stats, String readerGrp, String streamName,
                        int timeout, boolean writeAndRead, EventStreamClientFactory factory,
                        Stream stream) {
        super(consumerCount, events, secondsToRun, start, stats, readerGrp, timeout, writeAndRead, true);
        for (int i = 0; i < consumerCount; i++) {
            final String readerSt = "reader-" + i;
            this.readers.add(factory.createReader(
                    readerSt, readerGrp, new ByteArraySerializer(), ReaderConfig.builder().build()));
            log.info("add reader {}", readerSt);
        }
        this.readerExecutorService = Executors.newFixedThreadPool(consumerCount);
        this.producer = factory.createEventWriter(streamName,
                new ByteArraySerializer(),
                EventWriterConfig.builder().build());
        this.stream = stream;
        this.events = events;
    }

    @Override
    public byte[] readData() {
        log.error("call wrong method");
        return new byte[0];
    }

    @Override
    public void close() {
        readers.forEach(EventStreamReader::close);
        producer.close();
        validatorService.shutdownNow();
        futures.forEach(f -> f.cancel(true));
        readerExecutorService.shutdownNow();
        executorService.shutdownNow();
    }

    @Override
    public void EventsReaderPutback() {
        try {
            writeInitialEvents();
            validatorService.scheduleWithFixedDelay(() -> {
                Set<String> writtenEventsCopy = new HashSet<>(writtenEvents);
                writtenEventsCopy.forEach(e -> {
                    String[] tokens = e.split("-");
                    long timestamp = Long.parseLong(tokens[2]);
                    if (System.currentTimeMillis() - timestamp > EVENT_LOSS_THRESHOLD) {
                        Date date = new Date(timestamp);
                        Date curDate = new Date(System.currentTimeMillis());
                        log.error("WSCritical: event loss for {} , written time {}, current time {}",
                                new Object[]{e, date.toString(), curDate.toString()});
                    }
                });

            }, 1, 1, TimeUnit.MINUTES);
            log.info("written {} events, start loop back", events);
            readers.forEach(reader -> futures.add(readerExecutorService.submit(() -> {
                try {
                    final long msToRun = secondsToRun * 1000;
                    long time = System.currentTimeMillis();
                    while (!writtenEvents.isEmpty() && !Thread.interrupted()) {
                        EventRead<byte[]> event = reader.readNextEvent(timeout);
                        if (event.isCheckpoint()) {
                            log.info("received checkpoint {} with position {}", event.getCheckpointName(), event.getPosition().toString());
                        } else {
                            byte[] data = event.getEvent();
                            if (data != null) {
                                String received = new String(data, Charset.defaultCharset());
                                log.info("received event: {} at position {} at {} ", new Object[]{received, event.getPosition().toString(), System.currentTimeMillis()});
                                String[] tokens = received.split("-");
                                if (tokens.length != 3) {
                                    log.error("received event incorrect {}", received);
                                } else {
                                    synchronized (lock) {
                                        if (!writtenEvents.remove(received)) {
                                            writtenEvents.add(received);
                                            log.info("event {} not acked", received);
                                        }
                                    }
                                }
                                if ((time - startTime) < msToRun) {
                                    Thread.sleep(consumeTime + Math.abs((long) Math.floor(consumeTimeVariance * r.nextGaussian())));
                                    String id = tokens[0];
                                    long epoch = Long.parseLong(tokens[1]);
                                    if (epoch > currentEpoch.get()) {
                                        long old = currentEpoch.get();
                                        while (!currentEpoch.compareAndSet(old, epoch)) {
                                            old = currentEpoch.get();
                                        }
                                        log.info("increase epoch current epoch from {} to {}", old, currentEpoch.get());
                                    }
                                    epoch++;
                                    final String send = id + "-" + epoch + "-" + System.currentTimeMillis();
                                    producer.writeEvent(send.getBytes())
                                            .thenRunAsync(() -> {
                                                log.info("written event {} at {}", send, System.currentTimeMillis());
                                                synchronized (lock) {
                                                    if (!writtenEvents.add(send)) {
                                                        if (writtenEvents.remove(send)) {
                                                            log.info("event {} already received", send);
                                                        } else {
                                                            log.error("event {} remove failed", send);
                                                        }
                                                    }
                                                }
                                            }, executorService).join();
                                } else {
                                    log.info("stop write new events, and wait {} written events to be received", writtenEvents.size());
                                }
                        } else{
                            log.info("null event");
                        }
                    }
                    time = System.currentTimeMillis();
                }
            } catch(Exception e){
                log.error("met exception", e);
            } finally{
                log.info("close putback reader");
                close();
            }
        })));
        futures.forEach(f -> {
            try {
                f.get();
            } catch (InterruptedException e) {
                log.error("InterruptedException", e);
            } catch (ExecutionException e) {
                log.error("met exception", e);
            }
        });
        writtenEvents.forEach(e -> log.error("WSCritical: event loss for {} , final epoch {}", e, currentEpoch.get()));
    } catch(Throwable t) {
        log.error("met exception", t);
    } finally {
            log.info("finished events integrity test");
            try {
                while(true){
                    Thread.sleep(3600 * 1000);
                }
            }catch(Throwable e){
                log.error("met exception", e);
            }
        }
}

    private void writeInitialEvents() {
        for (int i = 0; i < events; i++) {
            final String send = "event" + i + "-" + 0 + "-" + System.currentTimeMillis();
            producer.writeEvent(send.getBytes())
                    .thenRunAsync(() -> log.info("written event {} at {}",
                            send, System.currentTimeMillis()), executorService).join();
            writtenEvents.add(send);
        }
    }
}
