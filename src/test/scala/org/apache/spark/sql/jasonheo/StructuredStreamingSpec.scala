package org.apache.spark.sql.jasonheo

import java.io.File
import java.sql.Timestamp
import java.util.TimeZone

import org.apache.commons.io.FileUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Encoders, Row, SparkSession}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.catalyst.util.DateTimeUtils.SQLTimestamp
import org.apache.spark.sql.execution.streaming.{ConsoleSinkProvider, MemoryStream, StreamExecution, StreamingQueryWrapper}
import org.apache.spark.sql.execution.streaming.sources.MemorySinkV2
import org.apache.spark.sql.functions.{count, from_json, window}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.ManualClock
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FlatSpec}
import org.scalatest.Matchers.{be, _}

case class AccessLog(url: String, timestamp: Timestamp)

case class UrlPv(url: String, pv: Long, windowStart: Timestamp, windowEnd: Timestamp)

class StructuredStreamingSpec
  extends FlatSpec
    with Logging
    with BeforeAndAfter
    with BeforeAndAfterAll {
  behavior of "Structured Streaming Unit Test"

  Logger.getLogger("org.apache.spark.sql.jasonheo").setLevel(Level.INFO)

  val checkpointLocation = "/tmp/spark-structured-streaming-unit-test"

  val spark: SparkSession = SparkSession
    .builder()
    .config("spark.sql.shuffle.partitions", 1)
    .master("local[2]")
    .getOrCreate()

  var transformedDf: DataFrame = _
  var urlPvDf: DataFrame = _
  var streamQuery: StreamExecution = _

  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._

  implicit val ctx = spark.sqlContext

  val accessLog: MemoryStream[String] = MemoryStream[String]

  val customClock = new ManualClock(Util.getTimestampMs("2020-08-25T00:00:00+09:00"))

  val memorySink = new MemorySinkV2

  "Update Mode" should "contain only updated windows" in {
    accessLog.addData("""{"url": "url-01", "timestamp": "2020-08-25T00:07:00+09:00"}""")
    accessLog.addData("""{"url": "url-02", "timestamp": "2020-08-25T00:07:01+09:00"}""")
    accessLog.addData("""{"url": "url-01", "timestamp": "2020-08-25T00:07:02+09:00"}""")

    customClock.setTime(Util.getTimestampMs("2020-08-25T00:10:00+09:00"))
    streamQuery.processAllAvailable()

    val urlPv: Seq[UrlPv] = Util.memorySinkToUrlPv(memorySink)

    urlPv.size should be(2)
    urlPv.filter(_.url == "url-01").head.pv should be(2)
    urlPv.filter(_.url == "url-02").head.pv should be(1)
    urlPv.head.windowStart.toString should be ("2020-08-25 00:05:00.0")
    urlPv.head.windowEnd.toString should be ("2020-08-25 00:10:00.0")
  }

  "Watermark" should "drop too late logs" in {
    accessLog.addData("""{"url": "url-01", "timestamp": "2020-08-24T23:50:00+09:00"}""") // drop 대상
    accessLog.addData("""{"url": "url-02", "timestamp": "2020-08-25T00:10:00+09:00"}""")

    customClock.setTime(Util.getTimestampMs("2020-08-25T00:15:00+09:00"))
    streamQuery.processAllAvailable()

    val urlPv: Seq[UrlPv] = Util.memorySinkToUrlPv(memorySink)

    urlPv.size should be(1)
    urlPv.filter(_.url == "url-01").size should be(0)
    urlPv.head.url should be("url-02")
    urlPv.head.pv should be(1)
  }

  // 매 test 시작 시 수행
  before {
    memorySink.clear
  }

  // 매 test 종료 시 수행
  after {
  }

  // 전체 테스트 시작 전 1회 수행
  override def beforeAll: Unit = {
    logInfo("test started")

    FileUtils.deleteDirectory(new File(checkpointLocation))

    val schema: StructType = Encoders.product[AccessLog].schema

    transformedDf = accessLog.toDF
      .select(from_json('value, schema) as "data")
      .select("data.*")

    urlPvDf = transformedDf
      .withWatermark("timestamp", "10 minutes")
      .groupBy('url,
        window('timestamp, "5 minutes"))
      .agg(count('url).as("pv"))
      .select('url,
        'pv,
        'window.getField("start") as 'window_start,
        'window.getField("end") as "window_end")

    streamQuery = urlPvDf
      .sparkSession
      .streams
      .startQuery(
        userSpecifiedName = Some("structured-streaming-unit-test"),
        userSpecifiedCheckpointLocation = Some(checkpointLocation),
        df = urlPvDf,
        extraOptions = Map[String, String](),
        // console에 출력하고자하는 경우: sink = new ConsoleSinkProvider,
        sink = memorySink,

        outputMode = OutputMode.Update,
        recoverFromCheckpointLocation = false,
        trigger = org.apache.spark.sql.streaming.Trigger.ProcessingTime("0 seconds"),
        triggerClock = customClock
      )
      .asInstanceOf[StreamingQueryWrapper]
      .streamingQuery
  }

  // 전체 테스트 종료 후 1회 수행
  override def afterAll: Unit = {
    logInfo("test ended")

    streamQuery.stop

    FileUtils.deleteDirectory(new File(checkpointLocation))
  }
}

object Util {
  def getTimestampMs(datetime: String): Long = {
    val timestampUsec: SQLTimestamp = DateTimeUtils.stringToTimestamp(
      UTF8String.fromString(datetime),
      TimeZone.getTimeZone("KST")
    ).get

    timestampUsec / 1000
  }

  def memorySinkToUrlPv(memorySink: MemorySinkV2): Seq[UrlPv] = {
    memorySink.allData.map { case (row: Row) => {
      UrlPv(row.getAs[String]("url"),
        row.getAs[Long]("pv"),
        row.getAs[Timestamp]("window_start"),
        row.getAs[Timestamp]("window_end")
      )
    }
    }
  }
}
