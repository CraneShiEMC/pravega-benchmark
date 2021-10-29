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

import java.util.concurrent.CompletableFuture;

import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.impl.ByteArraySerializer;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.StreamCut;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.util.concurrent.RateLimiter;

/**
 * Class for Pravega writer/producer.
 */
public class PravegaWriterWorker extends WriterWorker {
    private static Logger log = LoggerFactory.getLogger(PravegaWriterWorker.class);

    final EventStreamWriter<byte[]> producer;

    private final long writeWatermarkPeriodMillis;

    // No guard is required for nextNoteTime because it is only used by one thread per instance.
    private long nextNoteTime = System.currentTimeMillis();
    private RateLimiter rateLimiter;
    private PravegaStreamHandler streamHandle;
    private StreamCut lastStreamCut;

    /**
     * Construct a PravegaWriterWorker.
     *
     * @param writeWatermarkPeriodMillis If 0, noteTime will be called after every event.
     *                             If -1, noteTime will never be called.
     *                             If >0, noteTime will be called with a period of this many milliseconds.
     */
    PravegaWriterWorker(int sensorId, int events, int EventsPerFlush, int secondsToRun,
                        boolean isRandomKey, int messageSize, long start,
                        PerfStats stats, String streamName, int eventsPerSec,
                        boolean writeAndRead, EventStreamClientFactory factory,
                        boolean enableConnectionPooling, long writeWatermarkPeriodMillis,PravegaStreamHandler streamHandle) {

        super(sensorId, events, EventsPerFlush,
                secondsToRun, isRandomKey, messageSize, start,
                stats, streamName, eventsPerSec, writeAndRead);
        log.info("PravegaWriterWorker enableConnectionPooling : {}", enableConnectionPooling);
        this.producer = factory.createEventWriter(streamName,
                new ByteArraySerializer(),
                EventWriterConfig.builder()
                        .retryAttempts(1)
                        .enableConnectionPooling(true)
                        .build());
        this.writeWatermarkPeriodMillis = writeWatermarkPeriodMillis;
        this.rateLimiter = RateLimiter.create(eventsPerSec);
        this.streamHandle = streamHandle;
    }

    @Override
    public long recordWrite(byte[] data, TriConsumer record) {
        CompletableFuture ret;
        final long time = System.currentTimeMillis();
        
        ret = producer.writeEvent(Long.toString(time).getBytes());
        ret.thenAccept(d -> {
            record.accept(time, System.currentTimeMillis(), data.length);
            log.info("event is written: {}", data.toString());
            if(lastStreamCut==null){
                lastStreamCut = streamHandle.getCurrentStreamInfo().getTailStreamCut();
            }
            if(!lastStreamCut.toString().equals(streamHandle.getCurrentStreamInfo().getTailStreamCut().toString())){
                log.info("tail stream cut changed");
            }
            lastStreamCut = streamHandle.getCurrentStreamInfo().getTailStreamCut();
        });
        noteTimePeriodically();
        return time;
    }

    @Override
    public void writeData(byte[] data) {
        producer.writeEvent(data);
        noteTimePeriodically();
    }

    private void noteTimePeriodically() {
        if (writeWatermarkPeriodMillis >= 0) {
            final long time = System.currentTimeMillis();
            if (time > nextNoteTime) {
                producer.noteTime(time);
                log.debug("noteTimePeriodically: noteTime({})", time);
                nextNoteTime += writeWatermarkPeriodMillis;
            }
        }
    }

    @Override
    public void flush() {
        producer.flush();
    }

    @Override
    public synchronized void close() {
        producer.close();
    }
}
