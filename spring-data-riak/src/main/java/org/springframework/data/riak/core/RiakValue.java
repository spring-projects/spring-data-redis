package org.springframework.data.riak.core;

/**
 * @author J. Brisbin <jon@jbrisbin.com>
 */
@SuppressWarnings({"unchecked"})
public class RiakValue<T> implements KeyValueStoreValue {

  private Object delegate;
  private KeyValueStoreMetaData metaData;

  public RiakValue(T delegate, KeyValueStoreMetaData metaData) {
    this.delegate = delegate;
    this.metaData = metaData;
  }

  public KeyValueStoreMetaData getMetaData() {
    return this.metaData;
  }

  public T get() {
    return (T) delegate;
  }
}
