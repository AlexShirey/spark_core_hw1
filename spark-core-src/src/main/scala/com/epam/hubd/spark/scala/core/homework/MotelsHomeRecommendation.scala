package com.epam.hubd.spark.scala.core.homework

import com.epam.hubd.spark.scala.core.homework.domain.{BidError, BidItem, EnrichedItem}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MotelsHomeRecommendation {

  val ERRONEOUS_DIR: String = "erroneous"
  val AGGREGATED_DIR: String = "aggregated"

  def main(args: Array[String]): Unit = {
    require(args.length == 4, "Provide parameters in this order: bidsPath, motelsPath, exchangeRatesPath, outputBasePath")

    val bidsPath = args(0)
    val motelsPath = args(1)
    val exchangeRatesPath = args(2)
    val outputBasePath = args(3)

    val sc = new SparkContext(new SparkConf().setAppName("motels-home-recommendation"))

    processData(sc, bidsPath, motelsPath, exchangeRatesPath, outputBasePath)

    sc.stop()
  }

  def processData(sc: SparkContext, bidsPath: String, motelsPath: String, exchangeRatesPath: String, outputBasePath: String) = {

    /**
      * Task 1:
      * Read the bid data from the provided file.
      */
    val rawBids: RDD[List[String]] = getRawBids(sc, bidsPath)

    /**
      * Task 1:
      * Collect the errors and save the result.
      * Hint: Use the BideError case class
      */
    val erroneousRecords: RDD[String] = getErroneousRecords(rawBids)
    erroneousRecords.saveAsTextFile(s"$outputBasePath/$ERRONEOUS_DIR")

    /**
      * Task 2:
      * Read the exchange rate information.
      * Hint: You will need a mapping between a date/time and rate
      */
    val exchangeRates: Map[String, Double] = getExchangeRates(sc, exchangeRatesPath)

    /**
      * Task 3:
      * Transform the rawBids and use the BidItem case class.
      * - Convert USD to EUR. The result should be rounded to 3 decimal precision.
      * - Convert dates to proper format - use formats in Constants util class
      * - Get rid of records where there is no price for a Losa or the price is not a proper decimal number
      */
    val bids: RDD[BidItem] = getBids(rawBids, exchangeRates)

    /**
      * Task 4:
      * Load motels data.
      * Hint: You will need the motels name for enrichment and you will use the id for join
      */
    val motels: RDD[(String, String)] = getMotels(sc, motelsPath)

    /**
      * Task5:
      * Join the bids with motel names and utilize EnrichedItem case class.
      * Hint: When determining the maximum if the same price appears twice then keep the first entity you found
      * with the given price.
      */
    val enriched: RDD[EnrichedItem] = getEnriched(bids, motels)
    enriched.saveAsTextFile(s"$outputBasePath/$AGGREGATED_DIR")
  }

  //Task 1_1 impl
  def getRawBids(sc: SparkContext, bidsPath: String): RDD[List[String]] = {

    //creating RDD from a file, bidsPath => RDD[String]
    val bidsTextFileRDD: RDD[String] = sc.textFile(bidsPath)

    //splitting each line in RDD to a separate values (one value = one column) using "," (Constants.DELIMITER),
    //convert Array to List, so each line in RDD is now a List of values, RDD[String] => RDD[List[String]]
    val rawBidsRRD: RDD[List[String]] = bidsTextFileRDD.map(x => x.split(Constants.DELIMITER).toList)
    rawBidsRRD
  }

  //Task 1_2 impl
  def getErroneousRecords(rawBids: RDD[List[String]]): RDD[String] = {

    //filter - filtering RDD that contains "ERROR"; RDD[List[String]] => RDD[List[String]]
    //map - create from filtered lines a BidError object (date, message); RDD[List[String]] => RDD[BidError]
    //map - adding to each BidError object count=1 (to count total number of the same errors in future steps), RDD[BidError] => RDD[BidError, 1]
    //reduceByKey - summarizing the count of the same errors, RDD[BidError, List(1, 1, ...)] => RDD[BidError, Int]
    //map - convert errors and their counts to String with removing "(" and ")" chars, RDD[BidError, Int] => RDD[String]
    val errors: RDD[String] = rawBids
      .filter(row => row(2).contains("ERROR"))
      .map(row => BidError(row(1), row(2)))
      .map(error => (error, 1))
      .reduceByKey((a, b) => a + b)
      .map(result => result.toString().replaceAll("[()]", ""))
    errors
  }

  //Task 2 impl
  def getExchangeRates(sc: SparkContext, exchangeRatesPath: String): Map[String, Double] = {

    //creating RDD from exchangeRatesPath,
    //splitting each line by DELIMITER and convert Array to List,  exchangeRatesPath => RDD[List[String]]
    val ratesTextFileRDD: RDD[List[String]] = sc.textFile(exchangeRatesPath)
      .map(x => x.split(Constants.DELIMITER).toList)

    //map - getting only zero(date) and third(rate) values from the list of values, RDD[List[String]] => RDD[(String, Double)]
    //collect - convert RDD to Map (key - date, value - rate), RDD[(String, Double)] => Map[String, Double]
    val resultMap: Map[String, Double] = ratesTextFileRDD
      .map(row => (row.head.toString, row(3).toDouble))
      .collect().toMap[String, Double]
    resultMap
  }

  //Task 3 impl
  def getBids(rawBids: RDD[List[String]], exchangeRates: Map[String, Double]): RDD[BidItem] = {

    // filter - get rid of lines contain errors
    // map - get only needed values from line -motelId, date, US, MX, CA
    // map - create BidItem object from each line with converting date to proper format and price from USD to EUR
    // flatMap - convert  RDD[Seq[BidItem]] to RDD[BidItem]
    // filter - get rid of BidItem objects with zero price (it means that in original record the price is not presented, or there is no rate mapping for this date)
    val result = rawBids
      .filter(row => !row(2).contains("ERROR"))
      .map(row => (row.head, row(1), row(5), row(6), row(8)))
      .map(rowFormatted => {
        val convertedDate = convertDate(rowFormatted._2)
        val rate = exchangeRates.getOrElse(rowFormatted._2, 0.0)
        Seq(
          BidItem(rowFormatted._1, convertedDate, Constants.TARGET_LOSAS(0), convertUSDtoEUR(rowFormatted._3, rate)),
          BidItem(rowFormatted._1, convertedDate, Constants.TARGET_LOSAS(1), convertUSDtoEUR(rowFormatted._5, rate)),
          BidItem(rowFormatted._1, convertedDate, Constants.TARGET_LOSAS(2), convertUSDtoEUR(rowFormatted._4, rate)))
      })
      .flatMap(bid => bid)
      .filter(bid => bid.price != 0.0)
    result
  }

  //Task 4 impl
  def getMotels(sc: SparkContext, motelsPath: String): RDD[(String, String)] = {

    // create RDD from text file
    // map - split each line with Delimiter
    // map - get only needed values from each line - motelId, motel name
    sc.textFile(motelsPath)
      .map(line => line.split(Constants.DELIMITER).toList)
      .map(row => (row(0), row(1)))
  }

  //Task 5 impl
  def getEnriched(bids: RDD[BidItem], motels: RDD[(String, String)]): RDD[EnrichedItem] = {

    //determine the key to join on (motelId), and join bids with motels
    val mappedBids: RDD[(String, BidItem)] = bids.map(bid => (bid.motelId, bid))
    val joined: RDD[(String, (BidItem, String))] = mappedBids.join(motels)

    //create EnrichedItem from joined RDDs and keep only the records which have the maximum prices for a given motelId/bidDate
    joined.map(t => EnrichedItem(t._1, t._2._2, t._2._1.bidDate, t._2._1.loSa, t._2._1.price))
      .groupBy(item => (item.motelId, item.bidDate))
      .map(grouped => grouped._2.maxBy(item => item.price))
  }

  // private method, convert date to proper format using Constants class
  private def convertDate(date: String): String = {

    Constants.INPUT_DATE_FORMAT.parseDateTime(date).toString(Constants.OUTPUT_DATE_FORMAT)
  }

  // private method, convert USD to EUR with rounding double value to 3 digits after dot, return 0.0 if price value is empty
  private def convertUSDtoEUR(priceUSD: String, rateEUR: Double): Double = {

    //if price is not presented (empty string), return 0.0
    if (priceUSD.isEmpty) {
      return 0.0
    }

    //convert USD to EUR
    val priceNotRounded = priceUSD.toDouble * rateEUR

    //round the value to 3 digits
    BigDecimal(priceNotRounded).setScale(3, BigDecimal.RoundingMode.HALF_UP).toDouble
  }
}