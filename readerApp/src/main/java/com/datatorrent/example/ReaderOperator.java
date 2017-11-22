package com.datatorrent.example;

import java.io.IOException;
import java.util.Iterator;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.Context;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.store.api.IndexRecord;
import com.datatorrent.store.api.Record;
import com.datatorrent.store.reader.impl.HDFSStoreReader;

public class ReaderOperator extends BaseOperator implements InputOperator
{
  public static final Logger LOG = LoggerFactory.getLogger(ReaderOperator.class);
  private transient Configuration conf;
  private transient HDFSStoreReader<POJO2> reader;
  private long lastLogTime = System.currentTimeMillis();
  @AutoMetric
  private int count;
  @AutoMetric
  private long nullRecordCount;
  @AutoMetric
  private int windowCount;
  @AutoMetric
  private long windowTime;
  @AutoMetric
  private long firstNullRecordTS;
  private transient Iterator<Record<POJO2>> itr;
  @AutoMetric
  private long totalTime;
  @AutoMetric
  private double avgLatencySequential;
  @AutoMetric
  private double recordsPerSecSequential;

  @AutoMetric
  public long metaRefreshCount;
  @AutoMetric
  public long metaRefreshTime;

  @AutoMetric
  public long metaRefreshReadNextRecord;
  @AutoMetric
  public long metaRefreshGetIndex;
  @AutoMetric
  public long metaRefreshOpenRecordStream;
  @AutoMetric
  public long metaRefreshSeekToLatest;

  @AutoMetric
  public long readNextRecordCount;
  @AutoMetric
  public long readNextRecordTime;

  @AutoMetric
  public long seekToTSCount;
  @AutoMetric
  public long seekToTSTime;

  @AutoMetric
  public long seekCount;
  @AutoMetric
  public long seekTime;

  @AutoMetric
  public long seekToLatestCount;
  @AutoMetric
  public long seekToLatestTime;

  @AutoMetric
  public long seekToLastReadCount;
  @AutoMetric
  public long seekToLastReadTime;

  @AutoMetric
  public long getIndexBeforeTSCount;
  @AutoMetric
  public long getIndexBeforeTSTime;

  public void setup(Context.OperatorContext context)
  {
    setupReader();
    setupIterator();
  }

  public void setupReader()
  {
    conf = new Configuration();
    try {
      reader = new HDFSStoreReader<>("/user/ajay/", conf);
      reader.init("benchmarkMetrics", conf);
    } catch (IOException e) {
      throw new RuntimeException("Exception in creating store", e);
    }
  }

  public void setupIterator()
  {
    long t0 = -1, t1 = -1;

    try {
      DateTimeFormatter f = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss");
      DateTime dateTime0 = null;
      DateTime dateTime1 = null;
  
      String whenReplaySetT0 = "2017-11-01T01:01:01";
      String whenReplaySetT1 = "2017-11-21T01:01:01";
      if (whenReplaySetT0 != null) {
        try {
          dateTime0 = f.parseDateTime(whenReplaySetT0); // TODO: warn about time string error
          t0 = dateTime0.getMillis();
          //            t0 = System.currentTimeMillis();
        } catch (Exception e) {
        }
      }
  
      if (whenReplaySetT1 != null) {
        try {
          dateTime1 = f.parseDateTime(whenReplaySetT1);
          t1 = dateTime1.getMillis();
        } catch (Exception e) {
        }
      }
  
      // poll offsetConsumer until done or break;
      IndexRecord indexRecord = null;
      indexRecord = reader.getIndexBeforeTimestamp(t0);
      if (indexRecord == null) {
        LOG.info("iterator initialized");
        throw new RuntimeException("Index record was null. Need to scan all data");
      }
      LOG.info("Seek info : partNo {} , offset {}", indexRecord.partNumber, indexRecord.offset);
      reader.seek(indexRecord.partNumber, indexRecord.offset);
      reader.seekToTimestamp(t0);
      itr = reader.iterator();
      LOG.info("iterator initialized");
    }  catch (IOException e) {
      throw new RuntimeException("Exception in reading record ", e);
    }
  }

  @Override
  public void beginWindow(long windowId)
  {
    // TODO Auto-generated method stub
    super.beginWindow(windowId);
    this.windowCount = 0;
    this.windowTime = 0;
  }
  public void doRead()
  {
    Record<POJO2> record = null;

    long start = System.currentTimeMillis();
    record = itr.next();
    long end = System.currentTimeMillis();
    if (record != null) {
      count++;
      windowCount++;
      windowTime += (end - start);
      totalTime += (end - start);
    } else {
      nullRecordCount++;
      if (firstNullRecordTS <= 0) {
        firstNullRecordTS = System.currentTimeMillis();
      }
    }
  }

  public void doRandomRead()
  {
    try {
      long baseTime = 1511164800000L;
      long diff = 75716000;
      long searchTS = baseTime + (int)Math.ceil(Math.random()*diff);
      IndexRecord indexRecord = null;
      long start = System.currentTimeMillis();
      indexRecord = reader.getIndexBeforeTimestamp(searchTS);
      if (indexRecord == null) {
        return;
      }
      reader.seek(indexRecord.partNumber, indexRecord.offset);
      reader.seekToTimestamp(searchTS);
      itr = reader.iterator();

      Record<POJO2> record = null;
      record = itr.next();
      long end = System.currentTimeMillis();
      if (record != null) {
        count++;
        windowCount++;
        windowTime += (end - start);
        totalTime += (end - start);
      }
    } catch (IOException e) {
      throw new RuntimeException("Exception in reading record ", e);
    }
  }

  public void teardownReader()
  {
    try {
      reader.close();
    } catch (IOException e) {
      throw new RuntimeException("Exception in closing writer", e);
    }
  }

  @Override
  public void emitTuples()
  {
    doRead();
  }

  @Override
  public void endWindow()
  {
    super.endWindow();
    avgLatencySequential = (totalTime * 1.0 / count);
    recordsPerSecSequential = count * 1000.0 / totalTime;
    this.metaRefreshCount = reader.metaRefreshCount;
    this.metaRefreshTime = reader.metaRefreshTime;
    this.metaRefreshOpenRecordStream = reader.metaRefreshOpenRecordStream;
    this.metaRefreshReadNextRecord = reader.metaRefreshOpenRecordStream;
    this.metaRefreshGetIndex = reader.metaRefreshGetIndex;
    this.metaRefreshSeekToLatest = reader.metaRefreshSeekToLatest;
    this.seekCount = reader.seekCount;
    this.seekTime = reader.seekTime;
    this.seekToLastReadCount = reader.seekToLastReadCount;
    this.seekToLastReadTime = reader.seekToLastReadTime;
    this.seekToLatestCount = reader.seekToLatestCount;
    this.seekToLatestTime = reader.seekToLatestTime;
    this.seekToTSCount = reader.seekToTSCount;
    this.seekToTSTime = reader.seekToTSTime;
    this.readNextRecordCount = reader.readNextRecordCount;
    this.readNextRecordTime = reader.readNextRecordTime;
    this.getIndexBeforeTSCount = reader.getIndexBeforeTSCount;
    this.getIndexBeforeTSTime = reader.getIndexBeforeTSTime;
    if (System.currentTimeMillis() - lastLogTime >= 300000) {
      LOG.info("Avg Latency : {}", this.avgLatencySequential);
      LOG.info("recordsPerSec: {}", this.recordsPerSecSequential);
      LOG.info("metaRefreshCount : {}", this.metaRefreshCount);
      LOG.info("metaRefreshTime : {}", this.metaRefreshTime);
      LOG.info("metaRefreshOpenRecordStream : {}", this.metaRefreshOpenRecordStream);
      LOG.info("metaRefreshReadNextRecord : {}", this.metaRefreshReadNextRecord);
      LOG.info("metaRefreshGetIndex : {}", this.metaRefreshGetIndex);
      LOG.info("metaRefreshSeekToLatest : {}", this.metaRefreshSeekToLatest);
      LOG.info("seekCount : {}", this.seekCount);
      LOG.info("seekTime : {}", this.metaRefreshTime);
      LOG.info("seekToLastReadCount : {}", this.seekToLastReadCount);
      LOG.info("seekToLastReadTime : {}", this.seekToLastReadTime);
      LOG.info("seekToLatestCount : {}", this.seekToLatestCount);
      LOG.info("seekToLatestTime : {}", this.seekToLatestTime);
      LOG.info("seekToTSCount : {}", this.seekToTSCount);
      LOG.info("readNextRecordCount : {}", this.readNextRecordCount);
      LOG.info("readNextRecordTime : {}", this.readNextRecordTime);
      LOG.info("getIndexBeforeTSCount : {}", this.getIndexBeforeTSCount);
      LOG.info("getIndexBeforeTSTime : {}", this.getIndexBeforeTSTime);
      LOG.info("Record Count : {}", this.count);
      LOG.info("Record Read total time : {}", this.totalTime);
      LOG.info("nullRecordCount : {}", this.nullRecordCount);
      LOG.info("firstNullRecordTS : {}", this.firstNullRecordTS);
      LOG.info("nullRecordCount : {}", this.nullRecordCount);
      LOG.info("windowCount : {}", this.windowCount);
      LOG.info("windowTime : {}", this.windowTime);
      lastLogTime = System.currentTimeMillis();
    }
  }
}
