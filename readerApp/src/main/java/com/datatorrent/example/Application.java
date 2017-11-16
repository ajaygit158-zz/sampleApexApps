/**
 * Put your copyright and license info here.
 */
package com.datatorrent.example;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

@ApplicationAnnotation(name="StoreReader")
public class Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    ReaderOperator test1 = dag.addOperator("Test2", new ReaderOperator());
  }
}
