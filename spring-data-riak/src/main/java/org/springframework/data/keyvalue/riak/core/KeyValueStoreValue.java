package org.springframework.data.keyvalue.riak.core;

/**
 * A generic interface for dealing with values and their store metadata.
 *
 * @author J. Brisbin <jon@jbrisbin.com>
 */
public interface KeyValueStoreValue<T> {

  /**
   * Get the metadata associated with this value.
   *
   * @return
   */
  KeyValueStoreMetaData getMetaData();

  /**
   * Get the converted value itself.
   *
   * @return
   */
  T get();

}
