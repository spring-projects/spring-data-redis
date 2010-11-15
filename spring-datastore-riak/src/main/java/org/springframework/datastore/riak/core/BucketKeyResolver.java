package org.springframework.datastore.riak.core;

/**
 * @author J. Brisbin <jon@jbrisbin.com>
 */
public interface BucketKeyResolver {

  <V> boolean canResolve(V o);

  <B, K, V> BucketKeyPair<B, K> resolve(V o);
}
