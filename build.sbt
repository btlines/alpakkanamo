name := "alpakkanamo"

version := "0.0.2"

scalaVersion := "2.12.3"

crossScalaVersions := Seq("2.11.11", "2.12.3")

libraryDependencies ++= Seq(
  "com.amazonaws"      %  "aws-java-sdk-dynamodb"        % "1.11.196",
  "com.gu"             %% "scanamo"                      % "1.0.0-M1",
  "com.lightbend.akka" %% "akka-stream-alpakka-dynamodb" % "0.11",
  "org.typelevel"      %% "cats-core"                    % "1.0.0-MF",
  "org.scalatest"      %% "scalatest"                    % "3.0.1"     % Test,
  "com.typesafe.akka"  %% "akka-testkit"                 % "2.4.19"    % Test // same akka version as alpakka
)

organization := "beyondthelines"

licenses := ("MIT", url("http://opensource.org/licenses/MIT")) :: Nil

bintrayOrganization := Some("beyondthelines")

bintrayPackageLabels := Seq("scala", "dynamodb", "akka", "alpakka", "scanamo", "aws")

dynamoDBLocalPort := 8042
startDynamoDBLocal := startDynamoDBLocal.dependsOn(compile in Test).value
test in Test := (test in Test).dependsOn(startDynamoDBLocal).value
testOnly in Test := (testOnly in Test).dependsOn(startDynamoDBLocal).evaluated
testOptions in Test += dynamoDBLocalTestCleanup.value
fork in Test := true
envVars in Test := Map(
  "AWS_ACCESS_KEY_ID" -> "dummy",
  "AWS_SECRET_KEY" -> "credentials"
)
