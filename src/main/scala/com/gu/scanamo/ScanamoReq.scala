package com.gu.scanamo

import com.amazonaws.services.dynamodbv2.model._
import com.gu.scanamo.query.{Query, UniqueKey, UniqueKeys}
import com.gu.scanamo.request._
import com.gu.scanamo.update.UpdateExpression

import scala.collection.JavaConverters._

object ScanamoReq {

  private val batchSize = 25

  def put[T](tableName: String)(item: T)(implicit f: DynamoFormat[T]): ScanamoPutRequest =
    ScanamoPutRequest(tableName, f.write(item), None)

  def putAll[T](tableName: String)(items: Set[T])(implicit f: DynamoFormat[T]): List[BatchWriteItemRequest] =
    items.grouped(batchSize).toList.map(batch =>
      new BatchWriteItemRequest().withRequestItems(Map(tableName -> batch.toList.map(i =>
          new WriteRequest().withPutRequest(new PutRequest().withItem(f.write(i).getM))
        ).asJava).asJava)
      )

  def deleteAll(tableName: String)(items: UniqueKeys[_]): List[BatchWriteItemRequest] =
    items.asAVMap.grouped(batchSize).toList.map { batch =>
      new BatchWriteItemRequest().withRequestItems(
        Map(tableName -> batch.toList
          .map(item =>
            new WriteRequest().withDeleteRequest(
              new DeleteRequest().withKey(item.asJava)))
            .asJava).asJava)
    }

  def get(tableName: String)(key: UniqueKey[_]): GetItemRequest =
    new GetItemRequest().withTableName(tableName).withKey(key.asAVMap.asJava)

  def getWithConsistency(tableName: String)(key: UniqueKey[_]): GetItemRequest =
    new GetItemRequest().withTableName(tableName).withKey(key.asAVMap.asJava).withConsistentRead(true)

  def getAll(tableName: String)(keys: UniqueKeys[_]): List[BatchGetItemRequest] =
    keys.asAVMap.grouped(batchSize).toList.map { batch =>
      new BatchGetItemRequest().withRequestItems(Map(tableName ->
        new KeysAndAttributes().withKeys(batch.map(_.asJava).asJava)
      ).asJava)
    }

  def delete(tableName: String)(key: UniqueKey[_]): ScanamoDeleteRequest =
    ScanamoDeleteRequest(tableName, key.asAVMap, None)

  def scan(tableName: String): ScanamoScanRequest =
    ScanamoScanRequest(tableName, None, ScanamoQueryOptions.default)

  def scanConsistent(tableName: String): ScanamoScanRequest =
    ScanamoScanRequest(tableName, None, ScanamoQueryOptions.default.copy(consistent = true))

  def scanWithLimit(tableName: String, limit: Int): ScanamoScanRequest =
    ScanamoScanRequest(tableName, None, ScanamoQueryOptions.default.copy(limit = Some(limit)))

  def scanIndex(tableName: String, indexName: String): ScanamoScanRequest =
    ScanamoScanRequest(tableName, Some(indexName), ScanamoQueryOptions.default)

  def scanIndexWithLimit(tableName: String, indexName: String, limit: Int): ScanamoScanRequest =
    ScanamoScanRequest(tableName, Some(indexName), ScanamoQueryOptions.default.copy(limit = Some(limit)))

  def query(tableName: String)(query: Query[_]): ScanamoQueryRequest =
    ScanamoQueryRequest(tableName, None, query, ScanamoQueryOptions.default)

  def queryConsistent(tableName: String)(query: Query[_]): ScanamoQueryRequest =
    ScanamoQueryRequest(tableName, None, query, ScanamoQueryOptions.default.copy(consistent = true))

  def queryWithLimit(tableName: String)(query: Query[_], limit: Int): ScanamoQueryRequest =
    ScanamoQueryRequest(tableName, None, query, ScanamoQueryOptions.default.copy(limit = Some(limit)))

  def queryIndex(tableName: String, indexName: String)(query: Query[_]): ScanamoQueryRequest =
    ScanamoQueryRequest(tableName, Some(indexName), query, ScanamoQueryOptions.default)

  def queryIndexWithLimit(tableName: String, indexName: String)(query: Query[_], limit: Int): ScanamoQueryRequest =
    ScanamoQueryRequest(tableName, Some(indexName), query, ScanamoQueryOptions.default.copy(limit = Some(limit)))

  def update(tableName: String)(key: UniqueKey[_])(update: UpdateExpression): ScanamoUpdateRequest =
    ScanamoUpdateRequest(
      tableName = tableName,
      key = key.asAVMap,
      updateExpression = update.expression,
      attributeNames = update.attributeNames,
      attributeValues = update.attributeValues,
      condition = None
    )



}
