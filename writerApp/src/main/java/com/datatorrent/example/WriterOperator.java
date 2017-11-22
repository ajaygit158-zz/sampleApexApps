package com.datatorrent.example;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.Context;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.store.api.Record;
import com.datatorrent.store.writer.impl.HDFSStoreWriter;

/**
 * Created by bhupesh on 6/11/17.
 */
public class WriterOperator extends BaseOperator implements InputOperator
{
  public static final Logger LOG = LoggerFactory.getLogger(WriterOperator.class);

  private transient HDFSStoreWriter writer;
  private transient Configuration conf;
  private long count;
  @AutoMetric
  private double avgLatency;
  @AutoMetric
  private double recordsPerSec;
  private long timeToWrite;
  private boolean isClosed = false;

  public void setup(Context.OperatorContext context)
  {
    setupWriter();
  }

  public void setupWriter()
  {
    conf = new Configuration();
    try {
      writer = new HDFSStoreWriter("/user/ajay/", conf, true);
      writer.init("benchmarkMetrics", conf);
      LOG.info("Starting : {}", System.currentTimeMillis());
    } catch (IOException e) {
      throw new RuntimeException("Exception in creating store", e);
    }
  }

  public void process()
  {
    try {
      byte[] p = new byte[100];
      //      POJO2 p = new POJO2(count);
      long start = System.currentTimeMillis();
      writer.writeRecord(new Record<byte[]>(p));
      long end = System.currentTimeMillis();
      count++;
      timeToWrite += (end - start);
    } catch (IOException e) {
      throw new RuntimeException("Exception in writing record ", e);
    }
  }

  public void teardown()
  {
    try {
      writer.close();
    } catch (IOException e) {
      throw new RuntimeException("Exception in closing writer", e);
    }
  }

  @Override
  public void emitTuples()
  {
    process();
  }

  @Override
  public void endWindow()
  {
    super.endWindow();
    avgLatency = (timeToWrite*1.0/count);
    recordsPerSec = count*1000.0/timeToWrite;
  }
}
