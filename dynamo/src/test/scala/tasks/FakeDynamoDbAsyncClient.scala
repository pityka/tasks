package tasks

import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import scala.jdk.CollectionConverters._

import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model._

object FakeDynamoDbAsyncClient {
  val MaxItemBytes: Int = 400 * 1024
  val MaxTransactionBytes: Int = 4 * 1024 * 1024
}

final class FakeDynamoDbAsyncClient(val tableName: String)
    extends DynamoDbAsyncClient {

  case class ItemKey(pk: String, sk: String)

  private def estimateItemBytes(
      attrs: Map[String, AttributeValue]
  ): Int = {
    def one(av: AttributeValue): Int = {
      var size = 0
      if (av.s() != null) size += av.s().getBytes("UTF-8").length
      if (av.n() != null) size += av.n().getBytes("UTF-8").length
      if (av.b() != null) size += av.b().asByteArray().length
      if (av.bool() != null) size += 1
      if (av.hasL)
        av.l().asScala.foreach(x => size += one(x))
      if (av.hasM)
        av.m().asScala.foreach { case (k, v) =>
          size += k.getBytes("UTF-8").length
          size += one(v)
        }
      if (av.hasSs)
        av.ss().asScala.foreach(s => size += s.getBytes("UTF-8").length)
      size
    }
    attrs.foldLeft(0) { case (acc, (k, v)) =>
      acc + k.getBytes("UTF-8").length + one(v)
    }
  }

  private val store
      : AtomicReference[Map[ItemKey, Map[String, AttributeValue]]] =
    new AtomicReference(Map.empty)

  val transactWriteCount = new AtomicInteger(0)
  val putItemCount = new AtomicInteger(0)
  val getItemCount = new AtomicInteger(0)
  val queryCount = new AtomicInteger(0)

  val beforeTransactWrite: AtomicReference[() => Unit] =
    new AtomicReference(() => ())

  def rawStore: Map[ItemKey, Map[String, AttributeValue]] = store.get()

  def currentItem(pk: String, sk: String): Option[Map[String, AttributeValue]] =
    store.get.get(ItemKey(pk, sk))

  def setItem(
      pk: String,
      sk: String,
      attrs: Map[String, AttributeValue]
  ): Unit =
    store.updateAndGet { current =>
      current + (ItemKey(pk, sk) -> attrs)
    }

  def serviceName(): String = "dynamodb"
  def close(): Unit = ()

  private def completed[A](value: A): CompletableFuture[A] =
    CompletableFuture.completedFuture(value)

  private def failed[A](t: Throwable): CompletableFuture[A] = {
    val cf = new CompletableFuture[A]()
    cf.completeExceptionally(t)
    cf
  }

  private def extractKey(
      item: Map[String, AttributeValue],
      key: Map[String, AttributeValue]
  ): ItemKey = {
    val pk = (item.get("pk").orElse(key.get("pk"))).map(_.s()).getOrElse("")
    val sk = (item.get("sk").orElse(key.get("sk"))).map(_.s()).getOrElse("")
    ItemKey(pk, sk)
  }

  override def putItem(
      request: PutItemRequest
  ): CompletableFuture[PutItemResponse] = this.synchronized {
    putItemCount.incrementAndGet()
    if (
      request.hasExpressionAttributeNames && request
        .expressionAttributeNames()
        .isEmpty
    )
      return failed(
        DynamoDbException
          .builder()
          .statusCode(400)
          .message("ExpressionAttributeNames must not be empty")
          .build()
      )
    if (
      request.hasExpressionAttributeValues && request
        .expressionAttributeValues()
        .isEmpty
    )
      return failed(
        DynamoDbException
          .builder()
          .statusCode(400)
          .message("ExpressionAttributeValues must not be empty")
          .build()
      )
    val itemBytes = estimateItemBytes(request.item().asScala.toMap)
    if (itemBytes > FakeDynamoDbAsyncClient.MaxItemBytes)
      return failed(
        DynamoDbException
          .builder()
          .statusCode(400)
          .message("Item size has exceeded the maximum allowed size")
          .build()
      )
    if (request.tableName() != tableName)
      return failed(
        new RuntimeException(s"wrong table: ${request.tableName()}")
      )
    val item = request.item().asScala.toMap
    val key = extractKey(item, Map.empty)
    val existing = store.get.get(key)
    val condition = Option(request.conditionExpression())
    val namesMap = Option(request.expressionAttributeNames())
      .map(_.asScala.toMap)
      .getOrElse(Map.empty[String, String])
    val valuesMap = Option(request.expressionAttributeValues())
      .map(_.asScala.toMap)
      .getOrElse(Map.empty[String, AttributeValue])
    val passes = evaluateCondition(condition, namesMap, valuesMap, existing)
    if (!passes)
      failed(
        ConditionalCheckFailedException
          .builder()
          .message("condition failed")
          .build()
      )
    else {
      store.updateAndGet(current => current + (key -> item))
      completed(PutItemResponse.builder().build())
    }
  }

  override def getItem(
      request: GetItemRequest
  ): CompletableFuture[GetItemResponse] = {
    getItemCount.incrementAndGet()
    val key = request.key().asScala.toMap
    val itemKey =
      ItemKey(
        key.get("pk").map(_.s()).getOrElse(""),
        key.get("sk").map(_.s()).getOrElse("")
      )
    store.get.get(itemKey) match {
      case None =>
        completed(GetItemResponse.builder().build())
      case Some(attrs) =>
        completed(GetItemResponse.builder().item(attrs.asJava).build())
    }
  }

  override def query(
      request: QueryRequest
  ): CompletableFuture[QueryResponse] = {
    queryCount.incrementAndGet()
    val values = Option(request.expressionAttributeValues())
      .map(_.asScala.toMap)
      .getOrElse(Map.empty[String, AttributeValue])
    val pk = values.get(":pk").map(_.s()).getOrElse("")
    val items = store.get.collect {
      case (k, attrs) if k.pk == pk => attrs
    }.toList
    val sorted = items.sortBy(
      _.getOrElse("sk", AttributeValue.builder().s("").build()).s()
    )
    completed(
      QueryResponse
        .builder()
        .items(sorted.map(_.asJava).asJava)
        .build()
    )
  }

  override def transactWriteItems(
      request: TransactWriteItemsRequest
  ): CompletableFuture[TransactWriteItemsResponse] = this.synchronized {
    beforeTransactWrite.get.apply()
    transactWriteCount.incrementAndGet()
    val items = request.transactItems().asScala.toList
    val emptyMapFailure = items.flatMap { tw =>
      def check(
          hasNames: Boolean,
          names: java.util.Map[String, String],
          hasValues: Boolean,
          values: java.util.Map[String, AttributeValue]
      ): Option[String] = {
        if (hasNames && names.isEmpty)
          Some("ExpressionAttributeNames must not be empty")
        else if (hasValues && values.isEmpty)
          Some("ExpressionAttributeValues must not be empty")
        else None
      }
      if (tw.put() != null)
        check(
          tw.put().hasExpressionAttributeNames,
          tw.put().expressionAttributeNames(),
          tw.put().hasExpressionAttributeValues,
          tw.put().expressionAttributeValues()
        )
      else if (tw.delete() != null)
        check(
          tw.delete().hasExpressionAttributeNames,
          tw.delete().expressionAttributeNames(),
          tw.delete().hasExpressionAttributeValues,
          tw.delete().expressionAttributeValues()
        )
      else if (tw.conditionCheck() != null)
        check(
          tw.conditionCheck().hasExpressionAttributeNames,
          tw.conditionCheck().expressionAttributeNames(),
          tw.conditionCheck().hasExpressionAttributeValues,
          tw.conditionCheck().expressionAttributeValues()
        )
      else None
    }.headOption
    emptyMapFailure match {
      case Some(msg) =>
        return failed(
          DynamoDbException
            .builder()
            .statusCode(400)
            .message(msg)
            .build()
        )
      case None => ()
    }

    val oversizedItem = items.flatMap { tw =>
      if (tw.put() != null) {
        val size = estimateItemBytes(tw.put().item().asScala.toMap)
        if (size > FakeDynamoDbAsyncClient.MaxItemBytes) Some(size) else None
      } else None
    }.headOption
    oversizedItem.foreach { _ =>
      return failed(
        DynamoDbException
          .builder()
          .statusCode(400)
          .message("Item size has exceeded the maximum allowed size")
          .build()
      )
    }

    val totalBytes = items.foldLeft(0) { (acc, tw) =>
      acc + (if (tw.put() != null)
               estimateItemBytes(tw.put().item().asScala.toMap)
             else 0)
    }
    if (totalBytes > FakeDynamoDbAsyncClient.MaxTransactionBytes)
      return failed(
        DynamoDbException
          .builder()
          .statusCode(400)
          .message(
            "Transaction request cannot include multiple operations on one item; " +
              "or, aggregate size of items exceeds the limit"
          )
          .build()
      )

    val snapshot = store.get

    val evaluations: List[Either[CancellationReason, Map[ItemKey, Option[
      Map[String, AttributeValue]
    ]]]] =
      items.map { tw =>
        if (tw.put() != null) {
          val put = tw.put()
          val item = put.item().asScala.toMap
          val key = extractKey(item, Map.empty)
          val existing = snapshot.get(key)
          val condition = Option(put.conditionExpression())
          val names = Option(put.expressionAttributeNames())
            .map(_.asScala.toMap)
            .getOrElse(Map.empty[String, String])
          val values = Option(put.expressionAttributeValues())
            .map(_.asScala.toMap)
            .getOrElse(Map.empty[String, AttributeValue])
          if (evaluateCondition(condition, names, values, existing))
            Right(Map(key -> Some(item)))
          else
            Left(
              CancellationReason
                .builder()
                .code("ConditionalCheckFailed")
                .message("put condition failed")
                .build()
            )
        } else if (tw.delete() != null) {
          val del = tw.delete()
          val key = extractKey(Map.empty, del.key().asScala.toMap)
          val existing = snapshot.get(key)
          val condition = Option(del.conditionExpression())
          val names = Option(del.expressionAttributeNames())
            .map(_.asScala.toMap)
            .getOrElse(Map.empty[String, String])
          val values = Option(del.expressionAttributeValues())
            .map(_.asScala.toMap)
            .getOrElse(Map.empty[String, AttributeValue])
          if (evaluateCondition(condition, names, values, existing))
            Right(Map(key -> None))
          else
            Left(
              CancellationReason
                .builder()
                .code("ConditionalCheckFailed")
                .message("delete condition failed")
                .build()
            )
        } else if (tw.conditionCheck() != null) {
          val cc = tw.conditionCheck()
          val key = extractKey(Map.empty, cc.key().asScala.toMap)
          val existing = snapshot.get(key)
          val condition = Option(cc.conditionExpression())
          val names = Option(cc.expressionAttributeNames())
            .map(_.asScala.toMap)
            .getOrElse(Map.empty[String, String])
          val values = Option(cc.expressionAttributeValues())
            .map(_.asScala.toMap)
            .getOrElse(Map.empty[String, AttributeValue])
          if (evaluateCondition(condition, names, values, existing))
            Right(Map.empty)
          else
            Left(
              CancellationReason
                .builder()
                .code("ConditionalCheckFailed")
                .message("condition check failed")
                .build()
            )
        } else if (tw.update() != null) {
          Left(
            CancellationReason
              .builder()
              .code("NotSupported")
              .message("update not supported in fake")
              .build()
          )
        } else {
          Left(
            CancellationReason
              .builder()
              .code("None")
              .message("empty transact write item")
              .build()
          )
        }
      }

    if (evaluations.exists(_.isLeft)) {
      val reasons = evaluations.map {
        case Right(_) =>
          CancellationReason.builder().code("None").build()
        case Left(r) => r
      }
      failed(
        TransactionCanceledException
          .builder()
          .message("transaction cancelled")
          .cancellationReasons(reasons.asJava)
          .build()
      )
    } else {
      val mutations = evaluations.collect { case Right(m) => m }.flatten.toMap
      store.updateAndGet { current =>
        mutations.foldLeft(current) {
          case (acc, (key, Some(attrs))) => acc + (key -> attrs)
          case (acc, (key, None))        => acc - key
        }
      }
      completed(TransactWriteItemsResponse.builder().build())
    }
  }

  private def evaluateCondition(
      expression: Option[String],
      names: Map[String, String],
      values: Map[String, AttributeValue],
      existing: Option[Map[String, AttributeValue]]
  ): Boolean = expression match {
    case None => true
    case Some(expr) =>
      val trimmed = expr.trim
      if (trimmed == "attribute_not_exists(#pk)")
        existing.isEmpty
      else if (trimmed == "#ver = :ver") {
        val expected = values(":ver").n()
        existing.exists(attrs => attrs.get("ver").exists(_.n() == expected))
      } else
        throw new RuntimeException(s"unsupported condition expression: $expr")
  }

}
