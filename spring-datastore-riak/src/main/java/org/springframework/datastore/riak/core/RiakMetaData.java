package org.springframework.datastore.riak.core;

import org.springframework.http.MediaType;

import java.util.Map;

/**
 * @author J. Brisbin <jon@jbrisbin.com>
 */
public class RiakMetaData implements KeyValueStoreMetaData {

  private MediaType mediaType = MediaType.APPLICATION_JSON;
  private Map<String, Object> properties;

  public RiakMetaData(Map<String, Object> properties) {
    this.properties = properties;
  }

  public RiakMetaData(MediaType mediaType, Map<String, Object> properties) {
    this.mediaType = mediaType;
    this.properties = properties;
  }

  public MediaType getContentType() {
    return mediaType;
  }

  public Map<String, Object> getProperties() {
    return this.properties;
  }

}
