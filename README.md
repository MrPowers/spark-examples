# spark-examples

Working code to demonstrate key Spark concepts!

## [Testing Spark Applications](https://medium.com/@mrpowers/testing-spark-applications-8c590d3215fa#.bmktof284)

Custom transformations and user defined functions in Spark should be tested with the spark-testing-base library.

Check out the `basic` package for code snippets.

## [Chaining Custom DataFrame Transformations in Spark](https://medium.com/@mrpowers/chaining-custom-dataframe-transformations-in-spark-a39e315f903c#.yfylko1q2)

DataFrame transformations can be chained with implicit classes or the `Dataset#transform` method.

The examples in the `chaining` package show how to use both these methods and how to use the `Dataset#transform` for custom transformations that take arguments.