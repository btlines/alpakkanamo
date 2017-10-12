[![Build status](https://api.travis-ci.org/btlines/alpakkanamo.svg?branch=master)](https://travis-ci.org/btlines/alpakkanamo)
[![codecov](https://codecov.io/gh/btlines/alpakkanamo/branch/master/graph/badge.svg)](https://codecov.io/gh/btlines/alpakkanamo)
[![License](https://img.shields.io/:license-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![Download](https://api.bintray.com/packages/beyondthelines/maven/alpakkanamo/images/download.svg) ](https://bintray.com/beyondthelines/maven/alpakkanamo/_latestVersion)

# Alpakkanamo

A truly non-blocking Dynamo client (based on alpakka) for Scanamo.

An alpakka-based interpreter is now directly available in [Scanamo](https://github.com/guardian/scanamo).

## Context

Most of the Scala libraries for DynamoDB rely on the AWS Java client which offers limited performance (performs blocking HTTP calls):
* There's a good Scala libraries (e.g. [Scanamo](https://github.com/guardian/scanamo)) that unfortunately rely on the AWS Java client.
* There's a non-blocking library ([Alpakka](https://github.com/akka/alpakka) connector) that uses the AWS Java client interface.
This project aims at unifying these 2 libraries (hence the name) so that developpers can enjoy the best of both worlds: 
An elegant interface (Dynamo formats, ...) and good performances.

It provides 2 things:
* an interpreter for Scanamo based on the Alpakka connector for DynamoDB 
* a way to build AWS Dynamo requests using the DynamoFormats from Scanamo 

## Setup

In order to use Alpakkanamo you need to add this line to your `build.sbt`:

```scala
libraryDependencies += "beyondthelines" %% "alpakkanamo" % "0.0.2"
```

## Dependencies

Obviously Alpakkanamo depends on:
* Scanamo
* Alpakka (for the Dynamo connector)

## Usage

### Scanamo interpreter

To use the scanamo interpreter you first need to create an instance of it:

```scala
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.dynamodb.impl.DynamoSettings
import akka.stream.alpakka.dynamodb.scaladsl.DynamoClient

implicit val materializer = ActorMaterializer.create(system)
implicit val executor = system.dispatcher

// Create the Alpakka client that will be used by our Scanamo interpreter
val alpakkaClient = DynamoClient(
  DynamoSettings(
    region = "",
    host = "localhost",
    port = 8042,
    parallelism = 2
  )
)

// create some Dynamo operations using Scanamo
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType._
import com.gu.scanamo.query._
import com.gu.scanamo.syntax._

case class Forecast(location: String, weather: String, equipment: Option[String])

val forecasts = Table[Forecast]("forecast")

val ops = for {
  _ <- forecasts.putAll(Set(Forecast("London", "Rain", None), Forecast("Birmingham", "Sun", None)))
  _ <- forecasts.given('weather -> "Rain").update('location -> "London", set('equipment -> Some("umbrella")))
  _ <- forecasts.given('weather -> "Rain").update('location -> "Birmingham", set('equipment -> Some("umbrella")))
  results <- forecasts.scan()
} yield results

// execute these operations with the Alpakka client
val result: Future[List[Either[DynamoReadError, Forecast]]] = Alpakkanamo.exec(alpakkaClient)(ops)
```

More examples can be found in the unit tests: [AlpakkanamoSpec](https://github.com/btlines/alpakkanamo/blob/master/src/test/scala/com/gu/scanamo/AlpakkanamoSpec.scala)

### Building AWS requests using scanamo format

If you can't use the Scanamo interpreters to query Dynamo we can still use Scanamo to build standard Java AWS requests:

```scala
import com.amazonaws.services.dynamodbv2.model._

case class Farm(animals: List[String])
case class Farmer(name: String, age: Long, farm: Farm)

val boggis = Farmer("Boggis", 43L, Farm(List("chicken")))

val putRequest: PutItemRequest = ScanamoReq.put(tableName)(boggis).toAws
```

More examples can be found in the unit tests: [ScanamoReqSpec](https://github.com/btlines/alpakkanamo/blob/master/src/test/scala/com/gu/scanamo/ScanamoReqSpec.scala)

## More information

This library is fairly simple and more  implementation details can be found directly in the source code. 
Alternatively some implementation and motivation explanations can found [here](https://www.beyondthelines.net/databases/querying-dynamodb-from-scala/).



