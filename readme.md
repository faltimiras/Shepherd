# Shepherd

Group elements according "live" rules executed asynchronously or on sliding or tumbling time windows.

## Getting Started

Define your keyExtractor and your first rule (or use predetermined):
- Define a KeyExtractor to identify objects must be group. 
- Define a Rule

Then just create an instance of Shepherd

```
Shepherd shepherd = Shepherd.create()
    .basic(
        new FixedKeyExtractor(), 
        Optional.of(Arrays.asList(new AccumulateNRule(2))),
        listCollector)
    .build();

shepherd.add(1);
shepherd.add(2);
shepherd.add(2);
...
```

This stupid piece of code will just accumulate integers until 2, after second one is processed, values are available on resultPool.


**Key Extractor**

Objects grouped must have same key.
Key can be provided externally 

```
shepherd.add("the-key", 2);
```

Or via a java functional interface Function implementation


SimpleKeyExtractor and FixedKeyExtractor are already coded. Useful for simple cases, but common.

**Rule**

Decides when an new element must be accumulated or to a group or it has to be "released". 
Rules are checked when a new element is added to shepherd instance.

```
public interface Rule<V, S> {
	RuleResult canClose(Metadata metadata, V value, LazyValue<?, V, S> lazyValue);
}
```

- **metadata** keeps metadata for this still-open-group identified by they key.
- **v** is the new element that has been added.
- **lazyValue** give access to the rule to get already grouped elements.

- **RuleResult** tells to shepherd what has to be done with the new element added.
3 actions can be defined: **Append** (or not) the value just added, **Discard** stored until this moment, **Close** the group and "released".
RuleResult has a bunch of cool methods to build RuleResults easily.


There are 6 already coded simple rules to cover simple cases: [Streaming rules](https://github.com/faltimiras/Shepherd/tree/master/src/main/java/cat/altimiras/shepherd/rules/streaming)

**Callback**

It is the way to get brand new groups created.

Callback it is just a Consumer Java functional interface.

There is a simple already coded Callback that accumulates results: ListCollector

**Windows**

A part of streaming Rules that are evaluated when a new element is added, groups can be closed when a time window expires.

Decide if window must be closed or not it is decided by a RuleWindow that you can implement.

 ```
RuleResult canClose(Metadata metadata, LazyValue<?, V, S> lazyValue);
 ```

This rule is checked against every key every some time (configurable), to close or not groups according time.

By default some window rules are in place and to abstract class ready to extend to support easily **Sliding** and **Tumbling** windows. Nice explanation about them on [kafka stream](https://kafka.apache.org/20/documentation/streams/developer-guide/dsl-api.html#windowing) documentation.
Extend TumblingWindowBaseRule or SlidingWindowBaseRule to take advantage of their capabilities.
 
 ```
Shepherd shepherd = Shepherd.create()
    .basic(new SimpleKeyExtractor(),
         Optional.of(Arrays.asList(new NoDuplicatesRule())), 
         listCollector)
    .withWindow(
        Duration.ofMillis(5000), 
        new DiscardAllExpiredRule()
    ).build();
 ```
This piece of code removes duplicates during 5s windows. Basically first appearance is propagated, during next 5s, all appearances of it are silently removed.

**RuleExecutor**

Rule executor is responsible to apply the rules to an record. By default Rules are executed independently (IndependentExecutor) but you can also chain them (second rule receives toKeep values from previous rule) or you can implement your own.
To set up the executor, just: .setRuleExecutor(RuleExecutor ruleExecutor)

**Monitoring**

[Dropwizard metrics](https://metrics.dropwizard.io/) can be plugged to Shepherd.

 ```
import com.codahale.metrics.MetricRegistry

MetricRegistry metrics= ...

Shepherd shepherd = Shepherd.create()
    .basic(...)
    .withMonitoring(metrics)
 ```

**Storage**

Shepherd by default store "under construction" groups in memory, nevertheless it is configurable and storage can be totally custom via implementing ValuesStorage interface

There are 2 alternatively value storages ready to use: [Redis](https://redis.io/) and Files system.

Check examples on the tests.
 
## Built With

* [Maven](https://maven.apache.org/) - Dependency Management

## Authors

* **Ferran Altimiras** - [faltimiras](https://github.com/faltimiras)

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details