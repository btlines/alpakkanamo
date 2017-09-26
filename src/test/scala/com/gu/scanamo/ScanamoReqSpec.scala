package com.gu.scanamo

import com.amazonaws.services.dynamodbv2.model._
import com.gu.scanamo.update.UpdateExpression
import org.scalatest.{Matchers, WordSpec}

class ScanamoReqSpec extends WordSpec with Matchers {

  import collection.JavaConverters._
  import ops.AwsConverters._
  import syntax._

  case class Farm(animals: List[String])
  case class Farmer(name: String, age: Long, farm: Farm)

  val tableName = "farmers"
  val indexName = "animals"
  val boggis = Farmer("Boggis", 43L, Farm(List("chicken")))
  val bunce = Farmer("Bunce", 52L, Farm(List("goose")))
  val bean = Farmer("Bean", 55L, Farm(List("turkey")))

  "ScanamoReq" should {
    "create a PutItemRequest" in {
      ScanamoReq.put(tableName)(boggis).toAws shouldBe new PutItemRequest()
        .withTableName(tableName)
        .addItemEntry("name", new AttributeValue().withS(boggis.name))
        .addItemEntry("age", new AttributeValue().withN(boggis.age.toString))
        .addItemEntry("farm", new AttributeValue()
          .addMEntry("animals", new AttributeValue()
            .withL(boggis.farm.animals.map(new AttributeValue().withS(_)): _*)
          )
        )
    }

    "create list of BatchWriteItemRequest with PutRequest" in {
      ScanamoReq.putAll(tableName)(Set(boggis)) shouldBe List(
        new BatchWriteItemRequest().withRequestItems(
          Map(
            "farmers" -> List(
              new WriteRequest().withPutRequest(
                new PutRequest().withItem(
                  Map(
                    "name" -> new AttributeValue().withS(boggis.name),
                    "age" -> new AttributeValue().withN(boggis.age.toString),
                    "farm" -> new AttributeValue().addMEntry("animals", new AttributeValue()
                      .withL(boggis.farm.animals.map(new AttributeValue().withS(_)): _*)
                    )
                  ).asJava
                )
              )
            ).asJava
          ).asJava
        )
      )
    }

    "create list of BatchWriteItemRequest with DeleteRequest" in {
      ScanamoReq.deleteAll(tableName)('name -> Set(boggis.name)) shouldBe List(
        new BatchWriteItemRequest().withRequestItems(
          Map(
            "farmers" -> List(
              new WriteRequest().withDeleteRequest(
                new DeleteRequest().withKey(
                  Map(
                    "name" -> new AttributeValue().withS(boggis.name)
                  ).asJava
                )
              )
            ).asJava
          ).asJava
        )
      )
    }

    "create a GetItemRequest" in {
      ScanamoReq.get(tableName)('name -> bunce.name) shouldBe new GetItemRequest()
        .withTableName(tableName)
        .withKey(Map("name" -> new AttributeValue().withS(bunce.name)).asJava)
    }

    "create a GetItemRequest with read consistency" in {
      ScanamoReq.getWithConsistency(tableName)('name -> bunce.name) shouldBe new GetItemRequest()
        .withTableName(tableName)
        .withKey(Map("name" -> new AttributeValue().withS(bunce.name)).asJava)
        .withConsistentRead(true)
    }

    "create a list of BatchGetItemRequest" in {
      ScanamoReq.getAll(tableName)('name -> Set(bunce.name)) shouldBe List(
        new BatchGetItemRequest()
          .addRequestItemsEntry(
            "farmers",
            new KeysAndAttributes()
              .withKeys(Map("name" -> new AttributeValue().withS(bunce.name)).asJava)
          )
      )
    }

    "create a DeleteItemRequest" in {
      ScanamoReq.delete(tableName)('name -> bean.name).toAws shouldBe new DeleteItemRequest()
        .withTableName(tableName)
        .addKeyEntry("name", new AttributeValue().withS(bean.name))
    }

    "create a ScanRequest" in {
      ScanamoReq.scan(tableName).toAws shouldBe new ScanRequest()
        .withTableName(tableName)
        .withConsistentRead(false)
    }

    "create a ScanRequest with read consistency" in {
      ScanamoReq.scanConsistent(tableName).toAws shouldBe new ScanRequest()
        .withTableName(tableName)
        .withConsistentRead(true)
    }

    "create a ScanRequest with limit" in {
      ScanamoReq.scanWithLimit(tableName, 10).toAws shouldBe new ScanRequest()
        .withTableName(tableName)
        .withConsistentRead(false)
        .withLimit(10)
    }

    "create a ScanRequest on a secondary index" in {
      ScanamoReq.scanIndex(tableName, indexName).toAws shouldBe new ScanRequest()
        .withTableName(tableName)
        .withIndexName(indexName)
        .withConsistentRead(false)
    }

    "create a ScanRequest with limit on a secondary index" in {
      ScanamoReq.scanIndexWithLimit(tableName, indexName, 20).toAws shouldBe new ScanRequest()
        .withTableName(tableName)
        .withIndexName(indexName)
        .withConsistentRead(false)
        .withLimit(20)
    }

    "create a QueryRequest" in {
      ScanamoReq.query(tableName)('age -> 50).toAws shouldBe new QueryRequest()
        .withTableName(tableName)
        .withKeyConditionExpression("#K = :age")
        .withExpressionAttributeNames(Map("#K" -> "age").asJava)
        .withExpressionAttributeValues(Map(":age" -> new AttributeValue().withN("50")).asJava)
        .withConsistentRead(false)
    }

    "create a QueryRequest with read consistency" in {
      ScanamoReq.queryConsistent(tableName)('age -> 50).toAws shouldBe new QueryRequest()
        .withTableName(tableName)
        .withKeyConditionExpression("#K = :age")
        .withExpressionAttributeNames(Map("#K" -> "age").asJava)
        .withExpressionAttributeValues(Map(":age" -> new AttributeValue().withN("50")).asJava)
        .withConsistentRead(true)
    }

    "create a QueryRequest with limit" in {
      ScanamoReq.queryWithLimit(tableName)('age -> 50, 30).toAws shouldBe new QueryRequest()
        .withTableName(tableName)
        .withKeyConditionExpression("#K = :age")
        .withExpressionAttributeNames(Map("#K" -> "age").asJava)
        .withExpressionAttributeValues(Map(":age" -> new AttributeValue().withN("50")).asJava)
        .withConsistentRead(false)
        .withLimit(30)
    }

    "create a QueryRequest on a secondary index" in {
      ScanamoReq.queryIndex(tableName, indexName)('age -> 50).toAws shouldBe new QueryRequest()
        .withTableName(tableName)
        .withIndexName(indexName)
        .withKeyConditionExpression("#K = :age")
        .withExpressionAttributeNames(Map("#K" -> "age").asJava)
        .withExpressionAttributeValues(Map(":age" -> new AttributeValue().withN("50")).asJava)
        .withConsistentRead(false)
    }

    "create a QueryRequest on a secondary index with limit" in {
      ScanamoReq.queryIndexWithLimit(tableName, indexName)('age -> 50, 40).toAws shouldBe new QueryRequest()
        .withTableName(tableName)
        .withIndexName(indexName)
        .withKeyConditionExpression("#K = :age")
        .withExpressionAttributeNames(Map("#K" -> "age").asJava)
        .withExpressionAttributeValues(Map(":age" -> new AttributeValue().withN("50")).asJava)
        .withConsistentRead(false)
        .withLimit(40)
    }

    "create an UpdateItemRequest" in {
      ScanamoReq.update(tableName)('name -> bunce.name)(UpdateExpression.set('age, 32)).toAws shouldBe
        new UpdateItemRequest()
          .withTableName(tableName)
          .withKey(Map("name" -> new AttributeValue().withS(bunce.name)).asJava)
          .withReturnValues("ALL_NEW")
          .withUpdateExpression("SET #updateage = :update")
          .withExpressionAttributeNames(Map("#updateage" -> "age").asJava)
          .withExpressionAttributeValues(Map(":update" -> new AttributeValue().withN("32")).asJava)
    }
  }

}
