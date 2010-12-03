package org.springframework.data.keyvalue.riak.core;

/**
 * A generic class for specifying Quality Of Service parameters on operations.
 *
 * @author J. Brisbin <jon@jbrisbin.com>
 */
@SuppressWarnings({"unchecked"})
public class RiakQosParameters implements QosParameters {

  public Object readThreshold = null;
  public Object writeThreshold = null;
  public Object durableWriteThreshold = null;

  public <T> void setReadThreshold(T readThreshold) {
    this.readThreshold = readThreshold;
  }

  public <T> void setWriteThreshold(T writeThreshold) {
    this.writeThreshold = writeThreshold;
  }

  public <T> void setDurableWriteThreshold(T durableWriteThreshold) {
    this.durableWriteThreshold = durableWriteThreshold;
  }

  public <T> T getReadThreshold() {
    return (T) this.readThreshold;
  }

  public <T> T getWriteThreshold() {
    return (T) this.writeThreshold;
  }

  public <T> T getDurableWriteThreshold() {
    return (T) this.durableWriteThreshold;
  }

}
