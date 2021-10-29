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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.impl.ByteArraySerializer;
import io.pravega.client.stream.EventWriterConfig;
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
    final private Boolean isEnableRoutingKey;
    final private Boolean isBatch;
    final private int batchSize;
    private RateLimiter rateLimiter;


    // No guard is required for nextNoteTime because it is only used by one thread per instance.
    private long nextNoteTime = System.currentTimeMillis();
    
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
                        boolean enableConnectionPooling, long writeWatermarkPeriodMillis, AtomicLong[] seqNum,
                        Boolean isEnableRoutingKey, Boolean isBatch, int batchSize) {

        super(sensorId, events, EventsPerFlush,
                secondsToRun, isRandomKey, messageSize, start,
                stats, streamName, eventsPerSec, writeAndRead, seqNum, isEnableRoutingKey,isBatch,batchSize);

        this.producer = factory.createEventWriter(streamName,
                new ByteArraySerializer(),
                EventWriterConfig.builder()
                        .enableConnectionPooling(enableConnectionPooling)
                        .build());
        this.writeWatermarkPeriodMillis = writeWatermarkPeriodMillis;
        this.isEnableRoutingKey = isEnableRoutingKey;
        this.isBatch = isBatch;
        this.batchSize = batchSize;
        //this.rateLimiter = RateLimiter.create(eventsPerSec);
        log.info("events per producer {}, events per seconde {}", events, eventsPerSec);
    }

    @Override
    public long recordWrite(byte[] data, TriConsumer record) {
        CompletableFuture<Void> ret;
        final long time = System.currentTimeMillis();
        ret = writeEvent(producer, data);
        if(isBatch){
            ret.thenAccept(d -> {
                record.accept(time, System.currentTimeMillis(), data.length*batchSize);
                //log.info("[Batch write] single event size: {}, batch size: {}", data.length, batchSize);
            });
        }
        else{
            ret.thenAccept(d -> {
                record.accept(time, System.currentTimeMillis(), data.length);
                //log.info("Event write: {}", new String(data));
            });
        }
        noteTimePeriodically();
        return time;
    }

    @Override
    public void writeData(byte[] data) {
        writeEvent(producer, data).thenAccept(d -> {
            //log.info("Event write: {}", new String(data));
        });
        //producer.writeEvent(data);
        noteTimePeriodically();
    }


    private CompletableFuture<Void> writeEvent(EventStreamWriter<byte[]> producer, byte[] data) {
        CompletableFuture<Void> ret;
        if(isBatch){
            Random random = new Random();
            int number = random.nextInt(128);
            List<byte[]> eventList = new ArrayList<>();
            for (int i = 0; i < batchSize; i++) {
                eventList.add(data);
            }
            ret = producer.writeEvents(Integer.toString(number), eventList);
            //log.info("write batch action, event size {}, routing key {}", eventList.size(), number);
            return ret;
        }
        else if(isEnableRoutingKey) {
            String dataString = new String(data);
            String routingKey = dataString.split("-")[1];
            ret = producer.writeEvent(routingKey, data);
            
        } else {
            ret = producer.writeEvent(data);
        }
        return ret;
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
