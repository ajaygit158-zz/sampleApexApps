package com.datatorrent.example;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.store.api.Record;
import com.datatorrent.store.writer.impl.HDFSStoreWriter;

/**
 * Created by bhupesh on 6/11/17.
 */
public class WriterOperator  extends BaseOperator  implements InputOperator
{
  public static final Logger LOG = LoggerFactory.getLogger(WriterOperator.class);

  private transient HDFSStoreWriter<POJO2> writer;
  private transient Configuration conf;
  private int count;
  private boolean isClosed = false;

  public void setup(Context.OperatorContext context) {
    setupWriter();
  }

  public void setupWriter()
  {
    conf = new Configuration();
    try {
      writer = new HDFSStoreWriter<>("/tmp", conf, true);
      writer.init("TEST1", conf);
    } catch (IOException e) {
      throw new RuntimeException("Exception in creating store", e);
    }
  }

  public void process()
  {
    try {
      POJO2 p = new POJO2(count);
      LOG.info("{}",p);
      writer.writeRecord(new Record<>(p));
      count++;
    } catch (IOException e) {
      throw new RuntimeException("Exception in writing record " , e);
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
    if (count > 10000) {
      if (!isClosed) {
        teardown();
        isClosed = true;
      }
    } else {
      process();
    }
  }

}
