package org.springframework.data.riak.mapreduce;

import org.springframework.data.riak.core.BucketKeyPair;

/**
 * An implementation of {@link org.springframework.data.riak.mapreduce.MapReduceOperation}
 * to describe a Javascript language M/R function.
 *
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

  /**
   * Set the anonymous source to use for the M/R function.
   *
   * @param source
   */
  public void setSource(String source) {
    this.source = source;
  }

  public BucketKeyPair getBucketKeyPair() {
    return bucketKeyPair;
  }

  /**
   * Set the {@link org.springframework.data.riak.core.BucketKeyPair} to
   * point to for the Javascript to use in this M/R function.
   *
   * @param bucketKeyPair
   */
  public void setBucketKeyPair(BucketKeyPair bucketKeyPair) {
    this.bucketKeyPair = bucketKeyPair;
  }

  public Object getRepresentation() {
    return (null != bucketKeyPair ? bucketKeyPair : source);
  }

}
