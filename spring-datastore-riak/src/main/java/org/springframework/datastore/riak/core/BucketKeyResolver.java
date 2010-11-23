package org.springframework.datastore.riak.core;

/**
 * A generic interface to a resolver to turn a single object into a {@link
 * org.springframework.datastore.riak.core.BucketKeyPair}.
 *
 * @author J. Brisbin <jon@jbrisbin.com>
 */
public interface BucketKeyResolver {

  /**
   * Can this resolver deal with the given object?
   *
   * @param o
   * @param <V>
   * @return
   */
  <V> boolean canResolve(V o);

  /**
   * Turn the given object into a BucketKeyPair.
   *
   * @param o
   * @return
   */
  <B, K, V> BucketKeyPair<B, K> resolve(V o);
}
