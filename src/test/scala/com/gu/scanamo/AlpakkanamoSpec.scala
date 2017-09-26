package com.gu.scanamo

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.dynamodb.impl.DynamoSettings
import akka.stream.alpakka.dynamodb.scaladsl.DynamoClient
import akka.testkit.TestKit
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType._
import com.gu.scanamo.query._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, FunSpecLike, Matchers}

class AlpakkanamoSpec extends TestKit(ActorSystem("scanamo-alpakka"))
  with FunSpecLike
  with BeforeAndAfterAll
  with Matchers
  with ScalaFutures {

  override protected def afterAll(): Unit = {
    system.terminate()
    super.afterAll()
  }

  implicit val materializer = ActorMaterializer.create(system)
  implicit val executor = system.dispatcher
  implicit val defaultPatience =
    PatienceConfig(timeout = Span(2, Seconds), interval = Span(15, Millis))

  val client = LocalDynamoDB.client()
  val alpakkaClient = DynamoClient(
    DynamoSettings(
      region = "",
      host = "localhost",
      port = 8042,
      parallelism = 2
    )
  )

  it("should put asynchronously") {
    LocalDynamoDB.usingTable(client)("asyncFarmers")('name -> S) {
      case class Farm(asyncAnimals: List[String])
      case class Farmer(name: String, age: Long, farm: Farm)

      import com.gu.scanamo.syntax._

      val result = for {
        _ <- Alpakkanamo.put(alpakkaClient)("asyncFarmers")(Farmer("McDonald", 156L, Farm(List("sheep", "cow"))))
      } yield Scanamo.get[Farmer](client)("asyncFarmers")('name -> "McDonald")

      result.futureValue should equal(Some(Right(Farmer("McDonald", 156, Farm(List("sheep", "cow"))))))
    }
  }

  it("should get asynchronously") {
    LocalDynamoDB.usingTable(client)("asyncFarmers")('name -> S) {
      case class Farm(asyncAnimals: List[String])
      case class Farmer(name: String, age: Long, farm: Farm)

      Scanamo.put(client)("asyncFarmers")(Farmer("Maggot", 75L, Farm(List("dog"))))

      Alpakkanamo.get[Farmer](alpakkaClient)("asyncFarmers")(UniqueKey(KeyEquals('name, "Maggot")))
        .futureValue should equal(Some(Right(Farmer("Maggot", 75, Farm(List("dog"))))))

      import com.gu.scanamo.syntax._

      Alpakkanamo.get[Farmer](alpakkaClient)("asyncFarmers")('name -> "Maggot")
        .futureValue should equal(Some(Right(Farmer("Maggot", 75, Farm(List("dog"))))))
    }

    LocalDynamoDB.usingTable(client)("asyncEngines")('name -> S, 'number -> N) {
      case class Engine(name: String, number: Int)

      Scanamo.put(client)("asyncEngines")(Engine("Thomas", 1))

      import com.gu.scanamo.syntax._
      Alpakkanamo.get[Engine](alpakkaClient)("asyncEngines")('name -> "Thomas" and 'number -> 1)
        .futureValue should equal(Some(Right(Engine("Thomas", 1))))
    }
  }

  it("should get consistently asynchronously") {
    case class City(name: String, country: String)
    LocalDynamoDB.usingTable(client)("asyncCities")('name -> S) {

      import com.gu.scanamo.syntax._
      Alpakkanamo.put(alpakkaClient)("asyncCities")(City("Nashville", "US")).andThen {
        case _ =>
          Alpakkanamo.getWithConsistency[City](alpakkaClient)("asyncCities")('name -> "Nashville")
            .futureValue should equal(Some(Right(City("Nashville", "US"))))
      }
    }
  }

  it("should delete asynchronously") {
    LocalDynamoDB.usingTable(client)("asyncFarmers")('name -> S) {

      case class Farm(asyncAnimals: List[String])
      case class Farmer(name: String, age: Long, farm: Farm)

      Scanamo.put(client)("asyncFarmers")(Farmer("McGregor", 62L, Farm(List("rabbit"))))

      import com.gu.scanamo.syntax._

      val maybeFarmer = for {
        _ <- Alpakkanamo.delete(alpakkaClient)("asyncFarmers")('name -> "McGregor")
      } yield Scanamo.get[Farmer](client)("asyncFarmers")('name -> "McGregor")

      maybeFarmer.futureValue should equal(None)
    }
  }

  it("should deleteAll asynchronously") {
    LocalDynamoDB.usingTable(client)("asyncFarmers")('name -> S) {

      case class Farm(asyncAnimals: List[String])
      case class Farmer(name: String, age: Long, farm: Farm)

      import com.gu.scanamo.syntax._

      val dataSet = Set(
        Farmer("Patty", 200L, Farm(List("unicorn"))),
        Farmer("Ted", 40L, Farm(List("T-Rex"))),
        Farmer("Jack", 2L, Farm(List("velociraptor")))
      )

      Scanamo.putAll(client)("asyncFarmers")(dataSet)

      val maybeFarmer = for {
        _ <- Alpakkanamo.deleteAll(alpakkaClient)("asyncFarmers")('name -> dataSet.map(_.name))
      } yield Scanamo.scan[Farmer](client)("asyncFarmers")

      maybeFarmer.futureValue should equal(List.empty)
    }
  }

  it("should update asynchronously") {
    LocalDynamoDB.usingTable(client)("forecast")('location -> S) {

      case class Forecast(location: String, weather: String)

      Scanamo.put(client)("forecast")(Forecast("London", "Rain"))

      import com.gu.scanamo.syntax._

      val forecasts = for {
        _ <- Alpakkanamo.update(alpakkaClient)("forecast")('location -> "London", set('weather -> "Sun"))
      } yield Scanamo.scan[Forecast](client)("forecast")

      forecasts.futureValue should equal(List(Right(Forecast("London", "Sun"))))
    }
  }

  it("should update asynchornously if a condition holds") {
    LocalDynamoDB.usingTable(client)("forecast")('location -> S) {

      case class Forecast(location: String, weather: String, equipment: Option[String])

      val forecasts = Table[Forecast]("forecast")

      import com.gu.scanamo.syntax._

      val ops = for {
        _ <- forecasts.putAll(Set(Forecast("London", "Rain", None), Forecast("Birmingham", "Sun", None)))
        _ <- forecasts.given('weather -> "Rain").update('location -> "London", set('equipment -> Some("umbrella")))
        _ <- forecasts.given('weather -> "Rain").update('location -> "Birmingham", set('equipment -> Some("umbrella")))
        results <- forecasts.scan()
      } yield results

      Alpakkanamo.exec(alpakkaClient)(ops).futureValue should equal(
        List(Right(Forecast("London", "Rain", Some("umbrella"))), Right(Forecast("Birmingham", "Sun", None))))
    }
  }

  it("should scan asynchronously") {
    LocalDynamoDB.usingTable(client)("asyncBears")('name -> S) {

      case class Bear(name: String, favouriteFood: String)

      Scanamo.put(client)("asyncBears")(Bear("Pooh", "honey"))
      Scanamo.put(client)("asyncBears")(Bear("Yogi", "picnic baskets"))

      Alpakkanamo.scan[Bear](alpakkaClient)("asyncBears").futureValue.toList should equal(
        List(Right(Bear("Pooh", "honey")), Right(Bear("Yogi", "picnic baskets")))
      )
    }

    LocalDynamoDB.usingTable(client)("asyncLemmings")('name -> S) {

      case class Lemming(name: String, stuff: String)

      Scanamo.putAll(client)("asyncLemmings")(
        (for {_ <- 0 until 100} yield Lemming(util.Random.nextString(500), util.Random.nextString(5000))).toSet
      )

      Alpakkanamo.scan[Lemming](alpakkaClient)("asyncLemmings").futureValue.toList.size should equal(100)
    }
  }

  it("scans with a limit asynchronously") {
    case class Bear(name: String, favouriteFood: String)

    LocalDynamoDB.usingTable(client)("asyncBears")('name -> S) {
      Scanamo.put(client)("asyncBears")(Bear("Pooh", "honey"))
      Scanamo.put(client)("asyncBears")(Bear("Yogi", "picnic baskets"))
      val results = Alpakkanamo.scanWithLimit[Bear](alpakkaClient)("asyncBears", 1)
      results.futureValue should equal(List(Right(Bear("Pooh","honey"))))
    }
  }

  it ("scanIndexWithLimit") {
    case class Bear(name: String, favouriteFood: String, alias: Option[String])

    LocalDynamoDB.withTableWithSecondaryIndex(client)("asyncBears", "alias-index")('name -> S)('alias -> S) {
      Scanamo.put(client)("asyncBears")(Bear("Pooh", "honey", Some("Winnie")))
      Scanamo.put(client)("asyncBears")(Bear("Yogi", "picnic baskets", None))
      Scanamo.put(client)("asyncBears")(Bear("Graham", "quinoa", Some("Guardianista")))
      val results = Alpakkanamo.scanIndexWithLimit[Bear](alpakkaClient)("asyncBears", "alias-index", 1)
      results.futureValue should equal(List(Right(Bear("Graham","quinoa",Some("Guardianista")))))
    }
  }

  it("should query asynchronously") {
    LocalDynamoDB.usingTable(client)("asyncAnimals")('species -> S, 'number -> N) {

      case class Animal(species: String, number: Int)

      Scanamo.put(client)("asyncAnimals")(Animal("Wolf", 1))

      for {i <- 1 to 3} Scanamo.put(client)("asyncAnimals")(Animal("Pig", i))

      import com.gu.scanamo.syntax._

      Alpakkanamo.query[Animal](alpakkaClient)("asyncAnimals")('species -> "Pig").futureValue.toList should equal(
        List(Right(Animal("Pig", 1)), Right(Animal("Pig", 2)), Right(Animal("Pig", 3))))

      Alpakkanamo.query[Animal](alpakkaClient)("asyncAnimals")('species -> "Pig" and 'number < 3).futureValue.toList should equal(
        List(Right(Animal("Pig", 1)), Right(Animal("Pig", 2))))

      Alpakkanamo.query[Animal](alpakkaClient)("asyncAnimals")('species -> "Pig" and 'number > 1).futureValue.toList should equal(
        List(Right(Animal("Pig", 2)), Right(Animal("Pig", 3))))

      Alpakkanamo.query[Animal](alpakkaClient)("asyncAnimals")('species -> "Pig" and 'number <= 2).futureValue.toList should equal(
        List(Right(Animal("Pig", 1)), Right(Animal("Pig", 2))))

      Alpakkanamo.query[Animal](alpakkaClient)("asyncAnimals")('species -> "Pig" and 'number >= 2).futureValue.toList should equal(
        List(Right(Animal("Pig", 2)), Right(Animal("Pig", 3))))

    }

    LocalDynamoDB.usingTable(client)("asyncTransport")('mode -> S, 'line -> S) {

      case class Transport(mode: String, line: String)

      import com.gu.scanamo.syntax._

      Scanamo.putAll(client)("asyncTransport")(Set(
        Transport("Underground", "Circle"),
        Transport("Underground", "Metropolitan"),
        Transport("Underground", "Central")))

      Alpakkanamo.query[Transport](alpakkaClient)("asyncTransport")('mode -> "Underground" and ('line beginsWith "C")).futureValue.toList should equal(
        List(Right(Transport("Underground", "Central")), Right(Transport("Underground", "Circle"))))
    }
  }

  it ("queries with a limit asynchronously") {
    import com.gu.scanamo.syntax._

    case class Transport(mode: String, line: String)

    LocalDynamoDB.withTable(client)("transport")('mode -> S, 'line -> S) {
      Scanamo.putAll(client)("transport")(Set(
        Transport("Underground", "Circle"),
        Transport("Underground", "Metropolitan"),
        Transport("Underground", "Central")))
      val results = Alpakkanamo.queryWithLimit[Transport](alpakkaClient)("transport")('mode -> "Underground" and ('line beginsWith "C"), 1)
      results.futureValue should equal(List(Right(Transport("Underground","Central"))))
    }
  }

  it ("queries an index with a limit asynchronously") {
    case class Transport(mode: String, line: String, colour: String)

    import com.gu.scanamo.syntax._

    LocalDynamoDB.withTableWithSecondaryIndex(client)("transport", "colour-index")(
      'mode -> S, 'line -> S)('mode -> S, 'colour -> S
    ) {
      Scanamo.putAll(client)("transport")(Set(
        Transport("Underground", "Circle", "Yellow"),
        Transport("Underground", "Metropolitan", "Magenta"),
        Transport("Underground", "Central", "Red"),
        Transport("Underground", "Picadilly", "Blue"),
        Transport("Underground", "Northern", "Black")))
      val results = Alpakkanamo.queryIndexWithLimit[Transport](alpakkaClient)("transport", "colour-index")(
        'mode -> "Underground" and ('colour beginsWith "Bl"), 1)

      results.futureValue should equal(List(Right(Transport("Underground","Northern","Black"))))
    }
  }

  it ("queries an index asynchronously with 'between' sort-key condition") {
    case class Station(mode: String, name: String, zone: Int)

    import com.gu.scanamo.syntax._

    def deletaAllStations(client: DynamoClient, stations: Set[Station]) = {
      Alpakkanamo.delete(client)("stations")('mode -> "Underground")
      Alpakkanamo.deleteAll(client)("stations")(
        UniqueKeys(MultipleKeyList(('mode, 'name), stations.map(station => (station.mode, station.name))))
      )
    }
    val LiverpoolStreet = Station("Underground", "Liverpool Street", 1)
    val CamdenTown = Station("Underground", "Camden Town", 2)
    val GoldersGreen = Station("Underground", "Golders Green", 3)
    val Hainault = Station("Underground", "Hainault", 4)

    LocalDynamoDB.withTableWithSecondaryIndex(client)("stations", "zone-index")(
      'mode -> S, 'name -> S)('mode -> S, 'zone -> N
    ) {
      val stations = Set(LiverpoolStreet, CamdenTown, GoldersGreen, Hainault)
      Scanamo.putAll(client)("stations")(stations)
      val results1 = Alpakkanamo.queryIndex[Station](alpakkaClient)("stations", "zone-index")(
        'mode -> "Underground" and ('zone between (2 and 4)))

      results1.futureValue should equal(List(Right(CamdenTown), Right(GoldersGreen), Right(Hainault)))

      val maybeStations1 = for {_ <- deletaAllStations(alpakkaClient, stations)} yield Scanamo.scan[Station](client)("stations")
      maybeStations1.futureValue should equal(List.empty)

      Scanamo.putAll(client)("stations")(Set(LiverpoolStreet))
      val results2 = Alpakkanamo.queryIndex[Station](alpakkaClient)("stations", "zone-index")(
        'mode -> "Underground" and ('zone between (2 and 4)))
      results2.futureValue should equal(List.empty)

      val maybeStations2 = for {_ <- deletaAllStations(alpakkaClient, stations)} yield Scanamo.scan[Station](client)("stations")
      maybeStations2.futureValue should equal(List.empty)

      Scanamo.putAll(client)("stations")(Set(CamdenTown))
      val results3 = Alpakkanamo.queryIndex[Station](alpakkaClient)("stations", "zone-index")(
        'mode -> "Underground" and ('zone between (1 and 1)))
      results3.futureValue should equal(List.empty)
    }
  }

  it("queries for items that are missing an attribute") {
    case class Farmer(firstName: String, surname: String, age: Option[Int])

    import com.gu.scanamo.syntax._

    val farmersTable = Table[Farmer]("nursery-farmers")

    LocalDynamoDB.usingTable(client)("nursery-farmers")('firstName -> S, 'surname -> S) {
      val farmerOps = for {
        _ <- farmersTable.put(Farmer("Fred", "Perry", None))
        _ <- farmersTable.put(Farmer("Fred", "McDonald", Some(54)))
        farmerWithNoAge <- farmersTable.filter(attributeNotExists('age)).query('firstName -> "Fred")
      } yield farmerWithNoAge
      Alpakkanamo.exec(alpakkaClient)(farmerOps).futureValue should equal(List(Right(Farmer("Fred", "Perry", None))))
    }
  }

  it("should put multiple items asynchronously") {
    case class Rabbit(name: String)

    LocalDynamoDB.usingTable(client)("asyncRabbits")('name -> S) {
      val result = for {
        _ <- Alpakkanamo.putAll(alpakkaClient)("asyncRabbits")((
          for {_ <- 0 until 100} yield Rabbit(util.Random.nextString(500))
          ).toSet)
      } yield Scanamo.scan[Rabbit](client)("asyncRabbits")

      result.futureValue.toList.size should equal(100)
    }
    ()
  }

  it("should get multiple items asynchronously") {
    LocalDynamoDB.usingTable(client)("asyncFarmers")('name -> S) {

      case class Farm(animals: List[String])
      case class Farmer(name: String, age: Long, farm: Farm)

      Scanamo.putAll(client)("asyncFarmers")(Set(
        Farmer("Boggis", 43L, Farm(List("chicken"))), Farmer("Bunce", 52L, Farm(List("goose"))), Farmer("Bean", 55L, Farm(List("turkey")))
      ))

      Alpakkanamo.getAll[Farmer](alpakkaClient)("asyncFarmers")(
        UniqueKeys(KeyList('name, Set("Boggis", "Bean")))
      ).futureValue should equal(
        Set(Right(Farmer("Boggis", 43, Farm(List("chicken")))), Right(Farmer("Bean", 55, Farm(List("turkey"))))))

      import com.gu.scanamo.syntax._

      Alpakkanamo.getAll[Farmer](alpakkaClient)("asyncFarmers")('name -> Set("Boggis", "Bean")).futureValue should equal(
        Set(Right(Farmer("Boggis", 43, Farm(List("chicken")))), Right(Farmer("Bean", 55, Farm(List("turkey"))))))
    }

    LocalDynamoDB.usingTable(client)("asyncDoctors")('actor -> S, 'regeneration -> N) {
      case class Doctor(actor: String, regeneration: Int)

      Scanamo.putAll(client)("asyncDoctors")(
        Set(Doctor("McCoy", 9), Doctor("Ecclestone", 10), Doctor("Ecclestone", 11)))

      import com.gu.scanamo.syntax._
      Alpakkanamo.getAll[Doctor](alpakkaClient)("asyncDoctors")(
        ('actor and 'regeneration) -> Set("McCoy" -> 9, "Ecclestone" -> 11)
      ).futureValue should equal(
        Set(Right(Doctor("McCoy", 9)), Right(Doctor("Ecclestone", 11))))

    }
  }

  it("should get multiple items asynchronously (automatically handling batching)") {
    LocalDynamoDB.usingTable(client)("asyncFarms")('id -> N) {

      case class Farm(id: Int, name: String)
      val farms = (1 to 101).map(i => Farm(i, s"Farm #$i")).toSet

      Scanamo.putAll(client)("asyncFarms")(farms)

      Alpakkanamo.getAll[Farm](alpakkaClient)("asyncFarms")(
        UniqueKeys(KeyList('id, farms.map(_.id)))
      ).futureValue should equal(farms.map(Right(_)))
    }
  }

  it("conditionally put asynchronously") {
    case class Farm(animals: List[String])
    case class Farmer(name: String, age: Long, farm: Farm)

    import com.gu.scanamo.syntax._

    val farmersTable = Table[Farmer]("nursery-farmers")

    LocalDynamoDB.usingTable(client)("nursery-farmers")('name -> S) {
      val farmerOps = for {
        _ <- farmersTable.put(Farmer("McDonald", 156L, Farm(List("sheep", "cow"))))
        _ <- farmersTable.given('age -> 156L).put(Farmer("McDonald", 156L, Farm(List("sheep", "chicken"))))
        _ <- farmersTable.given('age -> 15L).put(Farmer("McDonald", 156L, Farm(List("gnu", "chicken"))))
        farmerWithNewStock <- farmersTable.get('name -> "McDonald")
      } yield farmerWithNewStock
      Alpakkanamo.exec(alpakkaClient)(farmerOps).futureValue should equal(
        Some(Right(Farmer("McDonald", 156, Farm(List("sheep", "chicken"))))))
    }
  }

  it("conditionally put asynchronously with 'between' condition") {
    case class Farm(animals: List[String])
    case class Farmer(name: String, age: Long, farm: Farm)

    import com.gu.scanamo.syntax._

    val farmersTable = Table[Farmer]("nursery-farmers")

    LocalDynamoDB.usingTable(client)("nursery-farmers")('name -> S) {
      val farmerOps = for {
        _ <- farmersTable.put(Farmer("McDonald", 55, Farm(List("sheep", "cow"))))
        _ <- farmersTable.put(Farmer("Butch", 57, Farm(List("cattle"))))
        _ <- farmersTable.put(Farmer("Wade", 58, Farm(List("chicken", "sheep"))))
        _ <- farmersTable.given('age between (56 and 57)).put(Farmer("Butch", 57, Farm(List("chicken"))))
        _ <- farmersTable.given('age between (58 and 59)).put(Farmer("Butch", 57, Farm(List("dinosaur"))))
        farmerButch <- farmersTable.get('name -> "Butch")
      } yield farmerButch
      Alpakkanamo.exec(alpakkaClient)(farmerOps).futureValue should equal(
        Some(Right(Farmer("Butch", 57, Farm(List("chicken")))))
      )
    }
  }

  it("conditionally delete asynchronously") {
    case class Gremlin(number: Int, wet: Boolean)

    import com.gu.scanamo.syntax._

    val gremlinsTable = Table[Gremlin]("gremlins")

    LocalDynamoDB.usingTable(client)("gremlins")('number -> N) {
      val ops = for {
        _ <- gremlinsTable.putAll(Set(Gremlin(1, false), Gremlin(2, true)))
        _ <- gremlinsTable.given('wet -> true).delete('number -> 1)
        _ <- gremlinsTable.given('wet -> true).delete('number -> 2)
        remainingGremlins <- gremlinsTable.scan()
      } yield remainingGremlins
      Alpakkanamo.exec(alpakkaClient)(ops).futureValue.toList should equal(List(Right(Gremlin(1,false))))
    }
  }

}
