package com.peedeex21.flink.training.datastream

import com.dataArtisans.flinkTraining.exercises.dataStreamJava.dataTypes.TaxiRide
import com.dataArtisans.flinkTraining.exercises.dataStreamJava.utils.{GeoUtils, TaxiRideGenerator, TaxiRideSchema}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.KafkaSink

/**
 * The task of the “Taxi Ride Cleansing” exercise is to cleanse a stream of TaxiRide records by removing records that
 * do not start or end in New York City. Erroneous records with invalid start and end location coordinates are
 * removed by this check as well. The cleansed TaxiRide stream should be written to Apache Kafka.
 */
object TaxiRideCleansing {

  // program arguments
  private var inputFile: Option[String] = None
  private var servingSpeedFactor: Option[Float] = None
  private var kafkaBroker: Option[String] = None
  private var kafkaTopic: Option[String] = None

  /**
   * Runs the TaxiRideCleansing program.
   *
   * @param args inputFile as String, servingSpeedFactor as Float, kafkaBroker as String, kafkaTopic as String
   */
  def main(args: Array[String]) {

    // read the program arguments
    if (!parseArguments(args)) {
      return
    }

    // set up the streaming execution environment
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment

    // add a source, taxi rides are read from file and streamed
    val rides = streamEnv.addSource(new TaxiRideGenerator(inputFile.get, servingSpeedFactor.get))

    // filter out all taxi rides which either not start or stop in the geo boundaries of NYC
    val filteredRides = rides.filter(ride => {
      GeoUtils.isInNYC(ride.startLon, ride.startLat) && GeoUtils.isInNYC(ride.endLon, ride.endLat)
    })

    // write the result to a local Kafka sink
    filteredRides.addSink(new KafkaSink[TaxiRide](kafkaBroker.get, kafkaTopic.get, new TaxiRideSchema()))

    // execute the program
    streamEnv.execute("Taxi Ride Cleansing")
  }

  /**
   * Checks and parses the program arguments.
   *
   * @param args inputFile as String, servingSpeedFactor as Float, kafkaBroker as String, kafkaTopic as String
   * @return true of right parameter are given, otherwise false
   */
  private def parseArguments(args: Array[String]): Boolean = {
    if (args.length != 4) {
      // there must be 4 arguments given
      printArgumentError()
      false
    } else {
      // parse the arguments
      try {
        inputFile = Some(args(0))
        servingSpeedFactor = Option(args(1).toFloat)
        kafkaBroker = Option(args(2))
        kafkaTopic = Option(args(3))
        true
      } catch {
        case e: NumberFormatException => printArgumentError()
          false
      }
    }
  }

  /**
   * Print the error message for wrongly given arguments.
   */
  private def printArgumentError() = println(
    """
      |Wrong number of arguments given.
      |   Usage: TaxiRideCleansing <inputFile> <servingSeedFactor> <kafkaBroker> <kafkaTopic>
    """.stripMargin)

}
