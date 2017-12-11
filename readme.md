# Shepherd

Group elements according your rules asynchronously, also checks periodically (the Dog) timeouted elements to group them according your rules too. 

## Getting Started

Define your keyExtractor and your first rule (or use predetermined):
- Define a KeyExtractor to identify objects must be group. 
- Define a Rule

Then just create an instance of Shepherd

```
Shepherd shepherd = Shepherd.create().basic(1, new SimpleKeyExtractor(), Optional.of(Arrays.asList(new AccumulateNRule(2))), resultsPool).build();
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
	RuleResult canGroup(Element<T> element);
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
- NoDuplicatesRule: In an stream of elements just "propagate" elements that are not equal to previous element (This rule only works alone and only with NoDuplicatesKeyExtractor )


**Callback**

Just a Consumer @FunctionalInterface

There is a simple already coded Callback that accumulates results: PoolerResult

**the Dog**

The Dog is the reaper that every certain time checks for elements that has been in the herd too much time.
To set up just tell to the dog maximum time elements can be in the herd and the rules to decide if elements must be group or not
 
 ```
Shepherd shepherd = Shepherd.create().basic(1, new SimpleKeyExtractor(), Optional.empty(), resultsPool).withDog(Duration.ofMillis(50), Arrays.asList(new AccumulateNRule(2))).build();
 ```
**RuleExecutor**

Rule executor is responsible to apply the rules to an element. By default Rules are executed independently (IndependentExecutor) but you can also chain them (second rule receives toKeep values from previous rule) or you can implement your own.
To set up the executor, just: .setRuleExecutor(RuleExecutor ruleExecutor)
 

## Built With

* [Maven](https://maven.apache.org/) - Dependency Management

## Authors

* **Ferran Altimiras** - [faltimiras](https://github.com/faltimiras)

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details
