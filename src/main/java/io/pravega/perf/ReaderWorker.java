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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

/**
 * An Abstract class for Readers.
 */
public abstract class ReaderWorker extends Worker implements Callable<Void> {
    private static Logger log = LoggerFactory.getLogger(ReaderWorker.class);
    final private static int MS_PER_SEC = 1000;
    final private Performance perf;
    final private boolean writeAndRead;
    final private int readDelay;

    ReaderWorker(int readerId, int events, int secondsToRun, long start,
                 PerfStats stats, String readerGrp, int timeout, boolean writeAndRead, int readDelay) {
        super(readerId, events, secondsToRun, 0, start, stats, readerGrp, timeout);

        this.writeAndRead = writeAndRead;
        this.readDelay = readDelay;
        this.perf = createBenchmark();

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
        final long msToRun = secondsToRun * MS_PER_SEC;
        byte[] ret = null;
        long time = System.currentTimeMillis();

        try {
            while ((time - startTime) < msToRun) {
                time = System.currentTimeMillis();
                ret = readData();
                if (ret != null) {
                    stats.recordTime(time, System.currentTimeMillis(), ret.length);
                }
            }
        } finally {
            close();
        }
    }


    public void EventsTimeReaderRW() throws IOException {
        final long msToRun = secondsToRun * MS_PER_SEC;
        byte[] ret = null;
        long time = System.currentTimeMillis();
        try {
            while ((time - startTime) < msToRun) {
                try {
                    long start = System.nanoTime();
                    ret = readData();
                    time = System.currentTimeMillis();
                    long end = System.nanoTime();
                    log.info("received event time: {}", end-start);
                    if (ret != null) {
//                        long startDeserialize = System.nanoTime();
//                        java.nio.ByteBuffer buf = java.nio.ByteBuffer.wrap(ret);
//                        long startDeserialize2 = System.nanoTime();
//                        Event event = Event.getRootAsEvent(buf);
//                        final long  start = event.header().executionTime();
//                        final String  routingKey = event.header().routingKey();
//                        final String  targetStream = event.header().targetStream();
//                        long endDeserialize = System.nanoTime();
//                        stats.recordTime(start, time, ret.length);
//                        log.info("execution time {}", start);
//                        log.info("routingKey {}", routingKey);
//                        log.info("targetStream {}", targetStream);
//                        log.info("deserialize time {}", endDeserialize - startDeserialize);
//                        log.info("deserialize time without buffer copy {}", endDeserialize - startDeserialize2);
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
