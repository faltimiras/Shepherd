# Shepherd

Group elements according your rules asynchronously, also checks periodically (the Dog) timeouted elements to group them according your rules too. 

## Getting Started

Define your keyExtractor and your first rule (or use predetermined):
- Define a KeyExtractor to identify objects must be group. 
- Define a Rule

Then just create an instance of Shepherd

```
Shepherd shepherd = Shepherd.create().basic(1, new SimpleKeyExtractor(), Optional.of(Arrays.asList(new AccumulateNRule(2))), listCollector).build();
shepherd.add(1);
shepherd.add(2);
shepherd.add(2);
...
```

This stupid piece of code will just accumulate integers until 2, after second one is processed, values are available on resultPooler.

**Key Extractor**

To distinguish between objects. Object will be group according his key.

```
public interface KeyExtractor<T> {
	Object key(T t);
}
```

SimpleKeyExtractor is already coded. It use same object as key. Useful for basic types.

**Rule**

To decide when objects accumulated can be group and "released".
```
public interface Rule<T> {
	RuleResult canGroup(Element<T> record);
}
```

Just return RuleResult according what rule decided:
- Can or Can't group
- If can group: witch elements want to "release" and witch elements want to keep
- If can not group: with elements want to keep

```
RuleResult.canGroup(..)
RuleResult.cantGroup(..) or RuleResult.canNotGroup(..) 
```

There are 2 already coded simple rules:
- AccumulatedNRule : Accumulates elements until desired amount is reached
- NoDuplicatesRule: In an stream of elements just "propagate" elements that are not equal to previous record (This rule only works alone and only with NoDuplicatesKeyExtractor )


**Callback**

Just a Consumer @FunctionalInterface

There is a simple already coded Callback that accumulates results: PoolerResult

**the Dog**

The Dog is the reaper that every certain time checks for elements that has been in the herd too much time.
To set up just tell to the dog maximum time elements can be in the herd and the rules to decide if elements must be group or not
 
 ```
Shepherd shepherd = Shepherd.create().basic(1, new SimpleKeyExtractor(), Optional.empty(), listCollector).withDog(Duration.ofMillis(50), Arrays.asList(new AccumulateNRule(2))).build();
 ```
**RuleExecutor**

Rule executor is responsible to apply the rules to an record. By default Rules are executed independently (IndependentExecutor) but you can also chain them (second rule receives toKeep values from previous rule) or you can implement your own.
To set up the executor, just: .setRuleExecutor(RuleExecutor ruleExecutor)

**Monitoring**

System can be monitored. An extra thread runs every X seconds to keep some system metrics.
There are 2 levels of monitoring: LOW and DEEP. DEEP provide more metrics but also can block system for more time.

Metrics available are:

LOW
* ELAPSED_TIME_COLLECTING_ms - Milliseconds gathering stats. Time system was block, not exactly. 
* NUM_ELEMENTS - Number of groups pending to group

DEEP (all LOW metrics plus)
* NUM_ELEMENTS_TOTAL - Total number of objects pending to groups
* AVG_ELEMENTS_GROUP - AVG elements inside groups  NUM_ELEMENTS_TOTAL/NUM_ELEMENTS
* MAX_ELEMENT_GROUP - Number of elements of the group with max number of elements
* MIN_ELEMENT_GROUP - Number of elements of the group with max number of elements
* OLDEST_ELEMENT - Birth time of the oldest record 
* AGE_OLDEST_ELEMENT_s - Age in seconds of the oldest record

 ```
.withMonitoring(new LogStatsListener()).level(Level.LOW)
 ```
 
## Built With

* [Maven](https://maven.apache.org/) - Dependency Management

## Authors

* **Ferran Altimiras** - [faltimiras](https://github.com/faltimiras)

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details
