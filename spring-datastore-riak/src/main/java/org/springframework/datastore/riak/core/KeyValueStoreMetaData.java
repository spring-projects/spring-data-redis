package org.springframework.datastore.riak.core;

import org.springframework.http.MediaType;

import java.util.Map;

/**
 * @author J. Brisbin <jon@jbrisbin.com>
 */
public interface KeyValueStoreMetaData {

  MediaType getContentType();

  Map<String, Object> getProperties();

}
