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
import Benchmark.Event.Header;
import Benchmark.Event.Type;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.flatbuffers.FlatBufferBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.pravega.client.stream.EventStreamWriter;

/**
 * An Abstract class for Readers.
 */
public abstract class ReaderWorker extends Worker implements Callable<Void> {
    private static Logger log = LoggerFactory.getLogger(ReaderWorker.class);
    final private static int MS_PER_SEC = 1000;
    final private Performance perf;
    final private boolean writeAndRead;
    final private int batchSize;
    final List<EventStreamWriter<byte[]>> producerList;
    final private EventStreamWriter<byte[]> producer;
    final private Random random = new Random();
    private PerfStats produceWriterStats;
    private AtomicInteger count = new AtomicInteger(0);
    final private boolean enableBatch;
    private int readDelay;

    ReaderWorker(int readerId, int events, int secondsToRun, long start,
                 PerfStats stats, String readerGrp, int timeout, boolean writeAndRead, int readDelay,int batchSize, List<EventStreamWriter<byte[]>> producerList, boolean enableBatch) {
        super(readerId, events, secondsToRun, 0, start, stats, readerGrp, timeout);

        this.writeAndRead = writeAndRead;
        this.readDelay = readDelay;
        this.perf = createBenchmark();
        this.batchSize = batchSize;
        this.producerList = producerList;
        log.info("producer list: {}", producerList.size());
        producer = producerList.get(0);
        produceWriterStats = new PerfStats("Writing", 5000, 120, null, null);
        this.enableBatch = enableBatch;

    }

    private void writeEvent(byte[] data){
        //log.info("writeEvent start time {}",System.nanoTime());
        //producerList.get(count.incrementAndGet() % 30).writeEvent(data);
       // producer.writeEvent(data);
       producerList.get(0).writeEvent(data);
        //log.info("writeEvent end time {}",System.nanoTime());
        // execute time: 18,614 us
    }

    private void batchWrite(ArrayList<byte[]> dataList){
        //log.info("batch event write start time {}",System.nanoTime());
        producerList.get(0).writeEvents("testing", dataList);
        //producer.writeEvents("testing", dataList);
        //log.info("batch event write end time {}",System.nanoTime());
       //execute time: 39,134,581 us
    }

    private Performance createBenchmark() {
        log.info("create benchmark for reader");
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
    public abstract byte[] readData();

    /**
     * close the consumer/reader.
     */
    public abstract void close();

    @Override
    public Void call() throws InterruptedException, ExecutionException, IOException {
        try {
            if(writeAndRead) {
                log.info("start sleep {} seconds", readDelay);
                Thread.sleep(readDelay * 1000);
            }
            log.info("run reader worker");
            perf.benchmark();
            log.info("complete reader worker");
        } catch (Exception e) {
            log.error("reader worker exception", e);
            throw e;
        }
        return null;
    }


    public void EventsReader() throws IOException {
        log.info("EventsReader: Running");
        byte[] ret = null;
        try {
            int i = 0;
            while (i < events) {
                final long startTime = System.currentTimeMillis();
                ret = readData();
                if (ret != null) {
                    stats.recordTime(startTime, System.currentTimeMillis(), ret.length);
                    i++;
                }
            }
        } finally {
            close();
        }
    }


    public void EventsReaderRW() throws IOException {
        log.info("EventsReaderRW: Running");
        final ByteBuffer timeBuffer = ByteBuffer.allocate(TIME_HEADER_SIZE);
        byte[] ret = null;
        try {
            int i = 0;
            while (i < events) {
                ret = readData();
                if (ret != null) {
                    final long endTime = System.currentTimeMillis();
                    timeBuffer.clear();
                    timeBuffer.put(ret, 0, TIME_HEADER_SIZE);
                    final long start = timeBuffer.getLong(0);
                    stats.recordTime(start, endTime, ret.length);
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
        byte[] ret = null;
        long time = System.currentTimeMillis();
        ArrayList<byte[]> eventList = new ArrayList<>(batchSize);
        FlatBufferBuilder builder = new FlatBufferBuilder(1024);
        try {
            while ((time - startTime) < msToRun) {
                time = System.currentTimeMillis();
                long startReadEvent = System.nanoTime();
                ret = readData();
                long endReadEvent = System.nanoTime();
                log.info("received event time {}",endReadEvent - startReadEvent);
                if (ret != null) {
                        long start = System.nanoTime();
                        java.nio.ByteBuffer buf = java.nio.ByteBuffer.wrap(ret);
                        Event event = Event.getRootAsEvent(buf);
                        final long  executionTime = event.header().executionTime();
                        final String  routingKey = event.header().routingKey();
                        final String  targetStream = event.header().targetStream();
                        ByteBuffer payload = event.payloadAsByteBuffer();
                        long end = System.nanoTime();
                        log.info("deserialize time {}", end - start);

                        long newStartTime = System.nanoTime();
                        int headerOffset = Header.createHeader(builder, Type.C2C,
                            builder.createString(targetStream),
                            builder.createString(routingKey),
                                executionTime);
                        int byteArrayOffset = builder.createByteVector(payload);
                        Event.startEvent(builder);
                        Event.addHeader(builder, headerOffset);
                        Event.addPayload(builder, byteArrayOffset);
                        int eventOffset = Event.endEvent(builder);
                        builder.finish(eventOffset);
                        byte[] newEvent =  builder.sizedByteArray();
                        long newEndTime = System.nanoTime();
                        log.info("serialize time {}", newEndTime - newStartTime);

                        if(enableBatch){
                            log.info("event list size {}", eventList.size());
                            if(eventList.size() >= batchSize){
                                log.info("do batch write");
                                batchWrite(eventList);
                                eventList.clear();
                                stats.recordTime(time, System.currentTimeMillis(), newEvent.length * batchSize);
                            }else{
                                eventList.add(newEvent);
                            }
                        }
                        else{
                            //writeEvent(newEvent);
                            writeEvent(ret);
                            log.info("event length {}",ret.length);
                            stats.recordTime(time, System.currentTimeMillis(), ret.length);
                        }
                    builder.clear();
                }
            }
        }
        catch(Exception e){
            log.info("fail to write event",e);
        }
        catch (Throwable t){
            log.info("fail to write event",t);
        }finally {
            close();
        }
    }


    public void EventsTimeReaderRW() throws IOException {
        log.info("EventsTimeReaderRW: Running");
        final long msToRun = secondsToRun * MS_PER_SEC;
        byte[] ret = null;
        long time = System.currentTimeMillis();
        try {
            while ((time - startTime) < msToRun) {
                try {
                    ret = readData();
                    time = System.currentTimeMillis();
                    if (ret != null) {
                        long startDeserialize = System.nanoTime();
                        long startDeserialize2 = System.nanoTime();
                        java.nio.ByteBuffer buf = java.nio.ByteBuffer.wrap(ret);
                        Event event = Event.getRootAsEvent(buf);
                        final long  start = event.header().executionTime();
                        final String  routingKey = event.header().routingKey();
                        final String  targetStream = event.header().targetStream();
                        long endDeserialize = System.nanoTime();
                        stats.recordTime(start, time, ret.length);
                        log.info("execution time {}", start);
                        log.info("routingKey {}", routingKey);
                        log.info("targetStream {}", targetStream);
                        log.info("deserialize time {}", endDeserialize - startDeserialize);
                        log.info("deserialize time without buffer copy {}", endDeserialize - startDeserialize2);
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