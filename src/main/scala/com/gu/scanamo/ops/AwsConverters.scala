package com.gu.scanamo.ops

import com.amazonaws.services.dynamodbv2.model._
import com.gu.scanamo.request._

object AwsConverters {

  implicit class ScanAwsReqConverter(req: ScanamoScanRequest) {
    def toAws: ScanRequest = JavaRequests.scan(req)
  }
  implicit class QueryAwsReqConverter(req: ScanamoQueryRequest)  {
    def toAws: QueryRequest = JavaRequests.query(req)
  }
  implicit class PutAwsReqConverter(req: ScanamoPutRequest) {
    def toAws: PutItemRequest = JavaRequests.put(req)
  }
  implicit class DeleteAwsReqConverter(req: ScanamoDeleteRequest) {
    def toAws: DeleteItemRequest = JavaRequests.delete(req)
  }
  implicit class UpdateAwsReqConverter(req: ScanamoUpdateRequest) {
    def toAws: UpdateItemRequest = JavaRequests.update(req)
  }

}
