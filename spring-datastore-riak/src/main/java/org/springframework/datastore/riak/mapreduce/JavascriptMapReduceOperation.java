package org.springframework.datastore.riak.mapreduce;

import org.springframework.datastore.riak.core.BucketKeyPair;

/**
 * @author J. Brisbin <jon@jbrisbin.com>
 */
public class JavascriptMapReduceOperation implements MapReduceOperation {

  protected String source;
  protected BucketKeyPair bucketKeyPair;

  public JavascriptMapReduceOperation(String source) {
    this.source = source;
  }

  public JavascriptMapReduceOperation(BucketKeyPair bucketKeyPair) {
    this.bucketKeyPair = bucketKeyPair;
  }

  public String getSource() {
    return source;
  }

  public void setSource(String source) {
    this.source = source;
  }

  public BucketKeyPair getBucketKeyPair() {
    return bucketKeyPair;
  }

  public void setBucketKeyPair(BucketKeyPair bucketKeyPair) {
    this.bucketKeyPair = bucketKeyPair;
  }

  public Object getRepresentation() {
    return (null != bucketKeyPair ? bucketKeyPair : source);
  }

}
