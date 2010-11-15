package org.springframework.datastore.riak.core;

/**
 * @author J. Brisbin <jon@jbrisbin.com>
 */
public interface BucketKeyPair<B, K> {

  B getBucket();

  K getKey();

}
