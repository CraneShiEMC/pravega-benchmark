/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.perf;

import Benchmark.Event.Event;
import io.pravega.client.stream.EventStreamWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * An Abstract class for Readers.
 */
public abstract class ReaderWorker extends Worker implements Callable<Void> {
    final private static int MS_PER_SEC = 1000;
    final private Performance perf;
    final private boolean writeAndRead;
    private static Logger log = LoggerFactory.getLogger(ReaderWorker.class);
    final private int batchSize;
    final List<EventStreamWriter<ByteBuffer>> producerList;
    final private EventStreamWriter<ByteBuffer> producer;
    final private Random random = new Random();
    private PerfStats produceWriterStats;
    private AtomicInteger count = new AtomicInteger(0);
    final private boolean enableBatch;
    ReaderWorker(int readerId, int events, int secondsToRun, long start,
                 PerfStats stats, String readerGrp, int timeout, boolean writeAndRead, int batchSize, List<EventStreamWriter<ByteBuffer>> producerList, boolean enableBatch) {
        super(readerId, events, secondsToRun, 0, start, stats, readerGrp, timeout);

        this.writeAndRead = writeAndRead;
        this.perf = createBenchmark();
        this.batchSize = batchSize;
        this.producerList = producerList;
        log.info("producer list: {}", producerList.size());
        producer = producerList.get(0);
        produceWriterStats = new PerfStats("Writing", 5000, 120, null, null);
        this.enableBatch = enableBatch;
    }

    private void writeEvent(ByteBuffer data){
        //log.info("writeEvent start time {}",System.nanoTime());
        //producerList.get(count.incrementAndGet() % 30).writeEvent(data);
        // producer.writeEvent(data);
        producerList.get(count.incrementAndGet() % 30).writeEvent(data);
        //log.info("writeEvent end time {}",System.nanoTime());
        // execute time: 18,614 us
    }

    private void batchWrite(ArrayList<ByteBuffer> dataList){
        //log.info("batch event write start time {}",System.nanoTime());
        producerList.get(count.incrementAndGet() % 30).writeEvents("testing", dataList);
        //producer.writeEvents("testing", dataList);
        //log.info("batch event write end time {}",System.nanoTime());
        //execute time: 39,134,581 us
    }

    private Performance createBenchmark() {
        final Performance perfReader;
        if (secondsToRun > 0) {
            perfReader = writeAndRead ? this::EventsTimeReaderRW : this::EventsTimeReader;
        } else {
            perfReader = writeAndRead ? this::EventsReaderRW : this::EventsReader;
        }
        return perfReader;
    }


    /**
     * read the data.
     */
    public abstract ByteBuffer readData();

    /**
     * close the consumer/reader.
     */
    public abstract void close();

    @Override
    public Void call() throws InterruptedException, ExecutionException, IOException {
        try {
            perf.benchmark();
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
        return null;
    }


    public void EventsReader() throws IOException {
        ByteBuffer ret = null;
        try {
            int i = 0;
            while (i < events) {
                final long startTime = System.currentTimeMillis();
                ret = readData();
                if (ret != null) {
                    stats.recordTime(startTime, System.currentTimeMillis(), ret.remaining());
                    i++;
                }
            }
        } finally {
            close();
        }
    }


    public void EventsReaderRW() throws IOException {
        final ByteBuffer timeBuffer = ByteBuffer.allocate(TIME_HEADER_SIZE);
        ByteBuffer ret = null;
        try {
            int i = 0;
            while (i < events) {
                ret = readData();
                if (ret != null) {
                    final long endTime = System.currentTimeMillis();
                    //timeBuffer.clear();
                    //timeBuffer.put(ret, 0, TIME_HEADER_SIZE);
                    final long start = ret.getLong(0);
                    stats.recordTime(start, endTime, ret.remaining());
                    i++;
                }
            }
        } finally {
            close();
        }
    }


    public void EventsTimeReader() throws IOException {
        log.info("EventsTimeReader: Running");
        final long msToRun = secondsToRun * MS_PER_SEC;
        ByteBuffer ret = null;
        long time = System.currentTimeMillis();
        ArrayList<ByteBuffer> eventList = new ArrayList<>();
        try {
            while ((time - startTime) < msToRun) {
                time = System.currentTimeMillis();
                ret = readData();
                time = System.currentTimeMillis();
                if (ret != null) {
                    try{
                    long startDeserialize = System.nanoTime();
                    //java.nio.ByteBuffer buf = java.nio.ByteBuffer.wrap(ret);
                    //long startDeserialize2 = System.nanoTime();
                    Event event = Event.getRootAsEvent(ret);
                    final long  start = event.header().executionTime();
                    final String  routingKey = event.header().routingKey();
                    final String  targetStream = event.header().targetStream();
                    long endDeserialize = System.nanoTime();
                        final ByteBuffer payload = event.payloadAsByteBuffer();
                        if(enableBatch){
                            if(eventList.size()>=batchSize){
                                batchWrite(eventList);
                                eventList.clear();
                                stats.recordTime(time, System.currentTimeMillis(), ret.remaining()*batchSize);
                            }else{
                                eventList.add(payload);
                            }
                        }
                        else{
                            writeEvent(payload);
                            stats.recordTime(time, System.currentTimeMillis(), ret.remaining());
                        }
                    }catch (Throwable t){
                        log.error("fail to get event",t);
                    }

                    // log.info("receive event {}", ret);
                    //log.info("read data time: {}", System.nanoTime());
                }
                // eventList.add(ret);
            }
        }
        catch(Exception e){
            log.error("fail to write event");
        }finally {
            close();
        }
    }


    public void EventsTimeReaderRW() throws IOException {
        final long msToRun = secondsToRun * MS_PER_SEC;
        ByteBuffer ret = null;
        long time = System.currentTimeMillis();
        try {
            while ((time - startTime) < msToRun) {
                try {
                    ret = readData();
                    time = System.currentTimeMillis();
                    if (ret != null) {
                        long startDeserialize = System.nanoTime();
                        //java.nio.ByteBuffer buf = java.nio.ByteBuffer.wrap(ret);
                        //long startDeserialize2 = System.nanoTime();
                        Event event = Event.getRootAsEvent(ret);
                        final long  start = event.header().executionTime();
                        final String  routingKey = event.header().routingKey();
                        final String  targetStream = event.header().targetStream();
                        long endDeserialize = System.nanoTime();
                        stats.recordTime(start, time, ret.remaining());
                        log.info("execution time {}", start);
                        log.info("routingKey {}", routingKey);
                        log.info("targetStream {}", targetStream);
                        log.info("deserialize time {}", endDeserialize - startDeserialize);
                        //log.info("deserialize time without buffer copy {}", endDeserialize - startDeserialize2);
                    }
                } catch (Exception e) {
                    log.error("read exception", e);
                }
            }
        } finally {
            close();
        }
    }
}
