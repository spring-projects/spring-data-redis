Spring Data - Key Value
=======================

The primary goal of the [Spring Data](http://www.springsource.org/spring-data) project is to make it easier to build Spring-powered applications that use new data access technologies such as non-relational databases, map-reduce frameworks, and cloud based data services.
As the name implies, the **Key Value** modules provides integration with key value stores such as [Redis](http://code.google.com/p/redis/) and [Riak](http://www.basho.com/Riak.html).

Examples
--------
For examples on using the Spring Data Key Value, see the dedicated project, also available on [GitHub](https://github.com/SpringSource/spring-data-keyvalue-examples)

Getting Help
------------

Read the main project [website](http://www.springsource.org/spring-data) and the [User Guide](http://static.springsource.org/spring-data/datastore-keyvalue/snapshot-site/reference/html/). Look at the source code and the [JavaDocs](http://static.springsource.org/spring-data/data-keyvalue/snapshot-site/apidocs/). For more detailed questions, use the [forum](http://forum.springsource.org/forumdisplay.php?f=80). If you are new to Spring as well as to Spring Data, look for information about [Spring projects](http://www.springsource.org/projects). 

# Quick Start


## Redis

For those in a hurry:

* Download the (SNAPSHOT) jar through Maven:

      <dependency>
        <groupId>org.springframework.data</groupId>
        <artifactId>spring-data-redis</artifactId>
        <version>1.0.0.BUILD-SNAPSHOT</version>
      </dependency> 

      <repository>
        <id>spring-maven-snapshot</id>
        <snapshots><enabled>true</enabled></snapshots>
        <name>Springframework Maven SNAPSHOT Repository</name>
        <url>http://maven.springframework.org/snapshot</url>
      </repository> 


* Configure the Redis connector to use (here [jedis](https://github.com/xetorthio/jedis)):

      <beans xmlns="http://www.springframework.org/schema/beans"
        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance 
        xmlns:p="http://www.springframework.org/schema/p"
        xsi:schemaLocation="
        http://www.springframework.org/schema/beans http://www.springframework.org/schema/beans/spring-beans.xsd">
        
        <bean id="jedisFactory" class="org.springframework.data.keyvalue.redis.connection.jedis.JedisConnectionFactory"/>
        
        <bean id="redisTemplate" class="org.springframework.data.keyvalue.redis.core.RedisTemplate"
            p:connection-factory="jedisFactory"/>
      </beans>

* Use `RedisTemplate` to interact with the Redis store:

      String random = template.randomKey();
      template.set(random, new Person("John", "Smith"));

* Use Redis 'views' to execute specific operations based on the underlying Redis type:

      ListOperations<String, Person> listOps = template.listOps();
      listOps.rightPush(random, new Person("Jane", "Smith"));
      List<Person> peopleOnSecondFloor = listOps.range("users:floor:2", 0, -1);

## Riak

* Download the jar through Maven:

      <dependency>
        <groupId>org.springframework.data</groupId>
        <artifactId>spring-data-riak</artifactId>
        <version>1.0.0.BUILD-SNAPSHOT</version>
      </dependency> 

      <repository>
        <id>spring-maven-snapshot</id>
        <snapshots><enabled>true</enabled></snapshots>
        <name>Springframework Maven SNAPSHOT Repository</name>
        <url>http://maven.springframework.org/snapshot</url>
      </repository> 

* Configure the `RiakTemplate` in your Spring ApplicationContext:

      <beans xmlns="http://www.springframework.org/schema/beans"
        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance 
        xmlns:p="http://www.springframework.org/schema/p"
        xsi:schemaLocation="http://www.springframework.org/schema/beans http://www.springframework.org/schema/beans/spring-beans.xsd">
  
        <bean id="riakTemplate" class="org.springframework.data.keyvalue.riak.core.RiakTemplate"
            p:defaultUri="http://localhost:8098/riak/{bucket}/{key}"
            p:mapReduceUri="http://localhost:8098/mapred"/>

      </beans>

* Use the `RiakTemplate` in your code:

      Java:
      -----
      
      MyObject obj = new MyObject("value1", "value2");
      riakTemplate.set("mybucket", "mykey", obj);
      
      Map returnObj = riakTemplate.getAsType("mybucket", "mykey", Map.class);
      
      Groovy:
      -----
      
      def obj = [first: "value1", second: "value2"]
      riakTemplate.set("mybucket", "mykey", obj)
      

Contributing to Spring Data
---------------------------

Here are some ways for you to get involved in the community:

* Get involved with the Spring community on the Spring Community Forums.  Please help out on the [forum](http://forum.springsource.org/forumdisplay.php?f=80) by responding to questions and joining the debate.
* Create [JIRA](https://jira.springframework.org/browse/DATAKV) tickets for bugs and new features and comment and vote on the ones that you are interested in.  
* Github is for social coding: if you want to write code, we encourage contributions through pull requests from [forks of this repository](http://help.github.com/forking/). If you want to contribute code this way, please reference a JIRA ticket as well covering the specific issue you are addressing.
* Watch for upcoming articles on Spring by [subscribing](http://www.springsource.org/node/feed) to springframework.org

Before we accept a non-trivial patch or pull request we will need you to sign the [contributor's agreement](https://support.springsource.com/spring_committer_signup).  Signing the contributor's agreement does not grant anyone commit rights to the main repository, but it does mean that we can accept your contributions, and you will get an author credit if we do.  Active contributors might be asked to join the core team, and given the ability to merge pull requests.
