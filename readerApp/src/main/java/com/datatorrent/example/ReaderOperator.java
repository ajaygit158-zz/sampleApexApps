package com.datatorrent.example;

import java.io.IOException;
import java.util.Iterator;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.store.api.IndexRecord;
import com.datatorrent.store.api.Record;
import com.datatorrent.store.reader.impl.HDFSStoreReader;

/**
 * Created by bhupesh on 6/11/17.
 */
public class ReaderOperator  extends BaseOperator  implements InputOperator
{
  public static final Logger LOG = LoggerFactory.getLogger(ReaderOperator.class);
  private transient Configuration conf;
  private transient HDFSStoreReader<POJO2> reader;
  private int count;
  private transient Iterator<Record<POJO2>> itr;

  public void setup(Context.OperatorContext context) {
    setupReader();
  }


  public void setupReader()
  {
    conf = new Configuration();
    try {
      reader = new HDFSStoreReader<>("/tmp", conf);
      reader.init("TEST1", conf);
    } catch (IOException e) {
      throw new RuntimeException("Exception in creating store", e);
    }
  }

  public void doRead()
  {
    try {
      if (count == 0) {
        long t0 = -1, t1 = -1;
  
        DateTimeFormatter f = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss");
        DateTime dateTime0 = null;
        DateTime dateTime1 = null;
  
        String whenReplaySetT0 = "2017-11-01T01:01:01";
        String whenReplaySetT1 = "2017-11-21T01:01:01";
        if (whenReplaySetT0 != null) {
          try {
            dateTime0 = f.parseDateTime(whenReplaySetT0); // TODO: warn about time string error
            t0 = dateTime0.getMillis();
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
        if (indexRecord == null)
        {
          LOG.info("iterator initialized");
          throw new RuntimeException("Index record was null. Need to scan all data");
        }
        LOG.info("Seek info : partNo {} , offset {}", indexRecord.partNumber, indexRecord.offset);
        reader.seek(indexRecord.partNumber, indexRecord.offset);
        reader.seekToTimestamp(t0);
        itr = reader.iterator();
        LOG.info("iterator initialized");
      }
      Record<POJO2> record = null;

      CONSUMER_LOOP:
        // get each record
        record = itr.next();
      if (record != null) {
        POJO2 pojo = record.getRecord();
        LOG.info(pojo.toString());
      }
    } catch (IOException e) {
      throw new RuntimeException("Exception in reading record " , e);
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
    count++;
  }

  @Override
  public void endWindow()
  {
    super.endWindow();
    if (count > 10000) {
      teardownReader();
      throw new ShutdownException();
    }
  }

}
