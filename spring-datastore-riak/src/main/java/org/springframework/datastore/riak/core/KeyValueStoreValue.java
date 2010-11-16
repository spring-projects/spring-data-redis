package org.springframework.datastore.riak.core;

/**
 * @author J. Brisbin <jon@jbrisbin.com>
 */
public interface KeyValueStoreValue<T> {

  KeyValueStoreMetaData getMetaData();

  T get();

}
