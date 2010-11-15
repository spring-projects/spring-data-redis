package org.springframework.datastore.riak.core;

/**
 * @author J. Brisbin <jon@jbrisbin.com>
 */
@SuppressWarnings({"unchecked"})
public class SimpleBucketKeyPair<B, K> implements BucketKeyPair {

  private Object bucket;
  private Object key;

  public SimpleBucketKeyPair(B bucket, K key) {
    this.bucket = bucket;
    this.key = key;
  }

  public B getBucket() {
    return (B) bucket;
  }

  public K getKey() {
    return (K) key;
  }
}
