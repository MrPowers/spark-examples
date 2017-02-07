name := "spark-examples"

spName := "mrpowers/spark-examples"

spShortDescription := "Spark examples"

spDescription := "Demonstrate how Spark works with tests!"

version := "0.0.1"

scalaVersion := "2.11.7"
sparkVersion := "2.1.0"

sparkComponents ++= Seq("sql","hive")

libraryDependencies ++= Seq(
  "com.holdenkarau" % "spark-testing-base_2.11" % "2.0.1_0.4.7"
)

parallelExecution in Test := false

// All Spark Packages need a license
licenses := Seq("MIT" -> url("http://opensource.org/licenses/MIT"))

credentials += Credentials(Path.userHome / ".ivy2" / ".sbtcredentials")