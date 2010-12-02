package org.springframework.data.riak.core;

/**
 * Specify Quality Of Service parameters.
 *
 * @author J. Brisbin <jon@jbrisbin.com>
 */
public interface QosParameters {

  /**
   * Instruct the server on the read threshold.
   *
   * @param <T>
   * @return
   */
  public <T> T getReadThreshold();

  /**
   * Instruct the server on the normal write threshold.
   *
   * @param <T>
   * @return
   */
  public <T> T getWriteThreshold();

  /**
   * Instruct the server on the durable write threshold.
   *
   * @param <T>
   * @return
   */
  public <T> T getDurableWriteThreshold();

}
