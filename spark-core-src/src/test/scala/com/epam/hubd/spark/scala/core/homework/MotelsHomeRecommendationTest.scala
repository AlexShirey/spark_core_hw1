package com.epam.hubd.spark.scala.core.homework

import java.io.File
import java.nio.file.Files

import com.epam.hubd.spark.scala.core.homework.MotelsHomeRecommendation.{AGGREGATED_DIR, ERRONEOUS_DIR}
import com.epam.hubd.spark.scala.core.homework.domain.{BidItem, EnrichedItem}
import com.epam.hubd.spark.scala.core.util.RddComparator
import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext, SparkContextProvider}
import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.junit._
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite}

/**
  * Created by Csaba_Bejan on 8/17/2016.
  */
class MotelsHomeRecommendationTest extends FunSuite with SharedSparkContext with RDDComparisons with BeforeAndAfter
  with BeforeAndAfterAll with SparkContextProvider {

  override def conf = new SparkConf().setMaster("local[2]").setAppName("motels-home-recommendation test")

  val INPUT_BIDS_SAMPLE = "src/test/resources/bids_sample.txt"
  val INPUT_RATES_SAMPLE = "src/test/resources/exchange_rate_sample.txt"
  val INPUT_MOTELS_SAMPLE = "src/test/resources/motels_sample.txt"

  val INPUT_BIDS_INTEGRATION = "src/test/resources/integration/input/bids.txt"
  val INPUT_EXCHANGE_RATES_INTEGRATION = "src/test/resources/integration/input/exchange_rate.txt"
  val INPUT_MOTELS_INTEGRATION = "src/test/resources/integration/input/motels.txt"

  val EXPECTED_AGGREGATED_INTEGRATION = "src/test/resources/integration/expected_output/aggregated"
  val EXPECTED_ERRORS_INTEGRATION = "src/test/resources/integration/expected_output/error_records"

  private var outputFolder: File = null

  before {
    outputFolder = Files.createTempDirectory("output").toFile
  }

  //test for task 1_1
  test("should read raw bids") {
    val expected = sc.parallelize(
      Seq(
        List("0000002", "15-04-08-2016", "0.89", "0.92", "1.32", "2.07", "", "1.35"),
        List("0000001", "06-05-02-2016", "ERROR_NO_BIDS_FOR_HOTEL")
      )
    )
    val rawBids = MotelsHomeRecommendation.getRawBids(sc, INPUT_BIDS_SAMPLE)
    assertRDDEquals(expected, rawBids)
  }

  //test for task 1_2
  test("should collect erroneous records") {
    val rawBids = sc.parallelize(
      Seq(
        List("1", "06-05-02-2016", "ERROR_1"),
        List("2", "15-04-08-2016", "0.89"),
        List("3", "07-05-02-2016", "ERROR_2"),
        List("4", "06-05-02-2016", "ERROR_1"),
        List("5", "06-05-02-2016", "ERROR_2")
      )
    )
    val expected = sc.parallelize(
      Seq(
        "06-05-02-2016,ERROR_1,2",
        "06-05-02-2016,ERROR_2,1",
        "07-05-02-2016,ERROR_2,1"
      )
    )
    val erroneousRecords = MotelsHomeRecommendation.getErroneousRecords(rawBids)
    assertRDDEquals(expected, erroneousRecords)
  }

  //test for task 2
  test("should read rates and return map") {
    val expected = Map("11-06-05-2016" -> 0.803, "11-05-08-2016" -> 0.873, "21-04-08-2016" -> 0.873)
    val exchangeRates = MotelsHomeRecommendation.getExchangeRates(sc, INPUT_RATES_SAMPLE)

    assert(expected, exchangeRates)
  }

  //test for task 3
  test("should convert USD to EUR, convert date, get rid of records where there is no price and return RDD[BidItem]") {
    val rawBids = sc.parallelize(
      Seq(
        List("0000008", "23-04-08-2016", "1.65", "0.91", "0.55", "0.76", "", "0.63", "0.44", "1.62", "1.19", "0.72", "2.06", "", "1.42", "", "0.53", "1.93"),
        List("0000001", "22-04-08-2016", "ERROR_INCONSISTENT_DATA"),
        //no price for "CA"
        List("0000004", "21-04-08-2016", "1.40", "", "2.04", "1.60", "1.79", "0.70", "", "0.42", "0.88", "0.31", "2.01", "1.25", "1.74", "1.90", "1.46", "1.25")
      )
    )

    //contains mapping only for date "21-04-08-2016"
    val exchangeRates = Map("11-06-05-2016" -> 0.803, "11-05-08-2016" -> 0.846, "21-04-08-2016" -> 0.873)

    //only "21-04-08-2016" date, US and MX
    val expected = sc.parallelize(
      Seq(
        BidItem("0000004", "2016-08-04 21:00", "US", 1.397), //1.60 * 0.873
        BidItem("0000004", "2016-08-04 21:00", "MX", 1.563) //1.79 * 0.873
      )
    )

    val bids = MotelsHomeRecommendation.getBids(rawBids, exchangeRates)

    assertRDDEquals(expected, bids)
  }


  //test for task 4
  test("should read motels and return only its id and name") {
    val expected = sc.parallelize(
      Seq(
        ("0000001", "Olinda Windsor Inn"),
        ("0000002", "Merlin Por Motel"),
        ("0000003", "Olinda Big River Casino")
      )
    )
    val motels = MotelsHomeRecommendation.getMotels(sc, INPUT_MOTELS_SAMPLE)
    assertRDDEquals(expected, motels)
  }


  //test for task 5
  test("should create EnrichedItems (BidItem + hotel name)") {

    val rddBids = sc.parallelize(
      Seq(
        BidItem("0000001", "2016-06-02 11:00", "MX", 1.50),
        BidItem("0000001", "2016-06-02 11:00", "US", 1.40),
        BidItem("0000001", "2016-06-02 11:00", "CA", 1.15),
        BidItem("0000005", "2016-06-02 12:00", "MX", 1.10),
        BidItem("0000005", "2016-06-02 12:00", "US", 1.20),
        BidItem("0000005", "2016-06-02 12:00", "CA", 1.30)
      )
    )

    val rddMotels = sc.parallelize(
      Seq(
        ("0000001", "Fantastic Hostel"),
        ("0000005", "Majestic Ibiza Por Hostel"),
        ("00000010", "Tree Hotel")
      )
    )

    val expected = sc.parallelize(
      Seq(
        EnrichedItem("0000001", "Fantastic Hostel", "2016-06-02 11:00", "MX", 1.50),
        EnrichedItem("0000005", "Majestic Ibiza Por Hostel", "2016-06-02 12:00", "CA", 1.30)
      )
    )

    val enrichedBids = MotelsHomeRecommendation.getEnriched(rddBids, rddMotels)

    assertRDDEquals(expected, enrichedBids)
  }

  //integration test
  test("should filter errors and create correct aggregates") {

    runIntegrationTest()

    //If the test fails and you are interested in what are the differences in the RDDs uncomment the corresponding line
    //    printRddDifferences(EXPECTED_ERRORS_INTEGRATION, getOutputPath(ERRONEOUS_DIR))
    //    printRddDifferences(EXPECTED_AGGREGATED_INTEGRATION, getOutputPath(AGGREGATED_DIR))

    assertRddTextFiles(EXPECTED_ERRORS_INTEGRATION, getOutputPath(ERRONEOUS_DIR))
    assertAggregatedFiles(EXPECTED_AGGREGATED_INTEGRATION, getOutputPath(AGGREGATED_DIR))
  }

  after {
    outputFolder.delete
  }

  private def runIntegrationTest() = {
    MotelsHomeRecommendation.processData(sc, INPUT_BIDS_INTEGRATION, INPUT_MOTELS_INTEGRATION, INPUT_EXCHANGE_RATES_INTEGRATION, outputFolder.getAbsolutePath)
  }

  private def assertRddTextFiles(expectedPath: String, actualPath: String) = {
    val expected = sc.textFile(expectedPath)
    val actual = sc.textFile(actualPath)
    assertRDDEquals(expected, actual)
  }

  private def assertAggregatedFiles(expectedPath: String, actualPath: String) = {
    val expected = parseLastDouble(sc.textFile(expectedPath)).collect.toMap
    val actual = parseLastDouble(sc.textFile(actualPath)).collect.toMap
    if (expected.size != actual.size) {
      Assert.fail(s"Aggregated have wrong number of records (${actual.size} instead of ${expected.size})")
    }
    expected.foreach(x => {
      val key = x._1
      val expectedValue = x._2
      if (!actual.contains(key)) {
        Assert.fail(s"Aggregated does not contain: $key,$expectedValue")
      }
      val actualValue = actual(key)
      if (Math.abs(expectedValue - actualValue) > 0.0011) {
        Assert.fail(s"Aggregated have different value for: $key ($actualValue instead of $expectedValue)")
      }
    })
  }

  private def parseLastDouble(rdd: RDD[String]) = {
    rdd.map(s => {
      val commaIndex = s.lastIndexOf(",")
      (s.substring(0, commaIndex), s.substring(commaIndex + 1).toDouble)
    })
  }

  private def printRddDifferences(expectedPath: String, actualPath: String) = {
    val expected = sc.textFile(expectedPath)
    val actual = sc.textFile(actualPath)
    RddComparator.printDiff(expected, actual)
  }

  private def getOutputPath(dir: String): String = {
    new Path(outputFolder.getAbsolutePath, dir).toString
  }
}
