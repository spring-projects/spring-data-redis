package org.springframework.datastore.redis.core;

import java.util.Set;

public interface SetOperations {

	boolean add(String key, String member);
	
	Set<String> getAll(String key);
	
	boolean remove(String key, String member);
	
	boolean removeRandom(String key);
	
	boolean moveBetweenSets(String srckey, String dstkey, String member);
	
	int size(String key);
	
	boolean contains(String key, String member);
	
	Set<String> getIntersectionOfSets(String... keys);
	
	void storeIntersectionOfSets(String dstkey, String... keys); 
	
	Set<String> getUnionOfSets(String... keys);
	
	void storeUnionOfSets(String dstkey, String... keys); 
	
	Set<String> getDifferenceBetweenSets(String... keys);
	
	void storeDifferenceBetweenSets(String dstkey, String... keys);
	
	String getRandom(String key);
	
}
