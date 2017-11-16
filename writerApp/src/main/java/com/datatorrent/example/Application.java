/**
 * Put your copyright and license info here.
 */
package com.datatorrent.example;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

@ApplicationAnnotation(name="StoreWriter")
public class Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    WriterOperator test1 = dag.addOperator("Test1", new WriterOperator());
  }
}
