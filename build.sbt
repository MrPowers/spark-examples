resolvers += "jitpack" at "https://jitpack.io"

name := "spark-examples"

spName := "mrpowers/spark-examples"

spShortDescription := "Spark examples"

spDescription := "Demonstrate how Spark works with tests!"

version := "0.0.1"

scalaVersion := "2.11.12"
sparkVersion := "2.2.0"

sparkComponents ++= Seq("sql","hive")

//spDependencies += "mrpowers/~-daria:2.2.0_0.16.0"
libraryDependencies += "com.github.MrPowers" % "spark-daria" % "v2.2.0_0.17.1"
libraryDependencies += "org.apache.commons" % "commons-text" % "1.1"
libraryDependencies += "MrPowers" % "spark-stringmetric" % "2.2.0_0.1.0"

libraryDependencies ++= Seq(
  "com.holdenkarau" % "spark-testing-base_2.11" % "2.2.0_0.7.4"
)

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.6" % "provided"

parallelExecution in Test := false

// All Spark Packages need a license
licenses := Seq("MIT" -> url("http://opensource.org/licenses/MIT"))

credentials += Credentials(Path.userHome / ".ivy2" / ".sbtcredentials")

fork in Test := true
envVars in Test := Map("PROJECT_ENV" -> "test")
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-Xss2M","-XX:+CMSClassUnloadingEnabled")