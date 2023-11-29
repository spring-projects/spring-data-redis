package org.springframework.data.redis.core.index;

import java.util.ArrayList;

public class TestIndexConfiguration extends IndexConfiguration {
  private static final String TESTPERSON_KEYSPACE = "TestPerson";

	@Override
  protected Iterable<IndexDefinition> initialConfiguration() {
      ArrayList<IndexDefinition> indexes = new ArrayList<IndexDefinition>();
      indexes.add(new SimpleIndexDefinition(TESTPERSON_KEYSPACE, "firstName"));
      
      //The sorted key on age
      indexes.add(new SortingIndexDefinition(TESTPERSON_KEYSPACE, "age"));
      //The sorted key on createdTimestamp
      indexes.add(new SortingIndexDefinition(TESTPERSON_KEYSPACE, "createdTimestamp"));
      
      //The composite index
      indexes.add(new CompositeSortingIndexDefinition<TestPerson>(TESTPERSON_KEYSPACE, "", new IndexNameHandler<TestPerson>(){
          @Override
          public String getIndexName(TestPerson p) {
              return TestAddress.getStateCategory(p.address.state);
          }
      }, new IndexValueHandler<TestPerson>(){
          @Override
          public Double getValue(TestPerson p) {
              return (double) p.createdTimestamp.getTime();
          }
      }));
      return indexes;
  }
}
