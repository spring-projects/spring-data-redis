package org.springframework.datastore.riak.core;

/**
 * A generic interface for representing composite keys in data stores that use a
 * bucket and key pair.
 *
 * @author J. Brisbin <jon@jbrisbin.com>
 */
public interface BucketKeyPair<B, K> {

  /**
   * Get the bucket representation.
   *
   * @return
   */
  B getBucket();

  /**
   * Get the key representation.
   *
   * @return
   */
  K getKey();

}
