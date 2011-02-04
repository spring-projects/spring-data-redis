package org.springframework.data.keyvalue.riak.mapreduce;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author J. Brisbin <jbrisbin@vmware.com>
 */
public class MapReduceLinkOperation implements MapReduceOperation {

	protected String bucket = null;
	protected String key;

	public MapReduceLinkOperation(String bucket, String key) {
		this.bucket = bucket;
		this.key = key;
	}

	public Object getRepresentation() {
		Map<String, Object> repr = new LinkedHashMap<String, Object>();
		repr.put("bucket", (null != bucket ? bucket : "_"));
		repr.put("key", (null != key ? key : "_"));
		return repr;
	}

}
