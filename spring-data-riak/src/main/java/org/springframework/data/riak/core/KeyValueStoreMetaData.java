package org.springframework.data.riak.core;

import org.springframework.http.MediaType;

import java.util.Map;

/**
 * A generic interface to MetaData provided by Key/Value data stores.
 *
 * @author J. Brisbin <jon@jbrisbin.com>
 */
public interface KeyValueStoreMetaData {

  /**
   * Get the Content-Type of this object.
   *
   * @return
   */
  MediaType getContentType();

  /**
   * Get the arbitrary properties for this object.
   *
   * @return
   */
  Map<String, Object> getProperties();

}
