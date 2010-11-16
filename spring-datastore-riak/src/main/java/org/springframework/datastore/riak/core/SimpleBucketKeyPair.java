package org.springframework.datastore.riak.core;

/**
 * @author J. Brisbin <jon@jbrisbin.com>
 */
@SuppressWarnings({"unchecked"})
public class SimpleBucketKeyPair<B, K> implements BucketKeyPair, Comparable {

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

  public int compareTo(Object o) {
    if (o instanceof SimpleBucketKeyPair) {
      SimpleBucketKeyPair pair = (SimpleBucketKeyPair) o;
      if (pair.getBucket().equals(bucket) && pair.getKey().equals(key)) {
        return 0;
      }
    }
    return -1;
  }
}
