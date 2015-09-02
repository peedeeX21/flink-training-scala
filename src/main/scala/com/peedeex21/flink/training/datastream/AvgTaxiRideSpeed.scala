package com.peedeex21.flink.training.datastream

import java.util.Properties

import com.dataArtisans.flinkTraining.exercises.dataStreamJava.dataTypes.TaxiRide
import com.dataArtisans.flinkTraining.exercises.dataStreamJava.utils.TaxiRideSchema
import org.apache.flink.api.common.functions.{FlatMapFunction, MapFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer082
import org.apache.flink.util.Collector
import org.joda.time.DateTime

import scala.collection.mutable

/**
 * The task of the “Average Ride Speed” exercise is to compute the average speed of taxi rides by matching and
 * combining their start and end records. The average speed is computed from the trip start time, which is contained
 * in the start record, and the trip end time and traveled distance, which are available in the end record.
 */
object AvgTaxiRideSpeed {

  // program arguments
  private var zookeeper: Option[String] = None
  private var groupId: Option[String] = None
  private var kafkaBroker: Option[String] = None
  private var kafkaTopic: Option[String] = None

  /**
   * Runs the AvgTaxiRideSpeed program.
   *
   * @param args zookeeper as String, groupId as String, kafkaBroker as String, kafkaTopic as String
   */
  def main(args: Array[String]) {

    // parse the program arguments
    if (!parseArguments(args)) {
      return
    }

    // set up streaming execution environment
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment

    // configure Kafka consumer
    val kafkaProperties = new Properties
    kafkaProperties.setProperty("zookeeper.connect", zookeeper.get)
    kafkaProperties.setProperty("group.id", groupId.get)
    kafkaProperties.setProperty("bootstrap.servers", kafkaBroker.get)

    // read the taxi rides from the kafka source
    val rides = streamEnv.addSource(new FlinkKafkaConsumer082[TaxiRide](
      kafkaTopic.get, new TaxiRideSchema, kafkaProperties))

    // compute the average speed of a taxi trip
    val rideSpeeds = rides.groupBy("rideId")
      .flatMap(new TaxiRideJoiner)
      .map(new SpeedComputer)

    // emit the result on stdout
    rideSpeeds.print

    // run the program
    streamEnv.execute("Average Taxi Ride Speed")
  }

  /**
   * Checks and parses the program arguments.
   *
   * @param args zookeeper as String, groupId as String, kafkaBroker as String, kafkaTopic as String
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
        zookeeper = Some(args(0))
        groupId = Option(args(1))
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
      |   Usage: object AvgTaxiRideSpeed <zookeeper> <groupId> <kafkaBroker> <kafkaTopic>
    """.stripMargin)

  /**
   * Convenient class to wrap essential information about a taxi trip.
   *
   * @param startTime start time of the trip
   * @param endTime end time of the trip
   * @param travelDistance traveled distance of the trip
   */
  case class TaxiTrip(id: Long, startTime: DateTime, endTime: DateTime, travelDistance: Float) {}

  /**
   * Reconstructs a taxi trip. Start events are buffered in a hash map until the end events occurs. A object of
   * [[TaxiTrip]].
   */
  class TaxiRideJoiner extends FlatMapFunction[TaxiRide, TaxiTrip] {

    // cache the start event of the taxi ride until the end event occurs
    private val startEvents = mutable.HashMap.empty[Long, TaxiRide]

    override def flatMap(ride: TaxiRide, collector: Collector[TaxiTrip]): Unit = {
      if (ride.isStart) {
        // cache the start event
        startEvents += (ride.rideId -> ride)
      } else {
        // match start and end event
        startEvents.remove(ride.rideId) match {
          case Some(startEvent) => collector.collect(
            new TaxiTrip(startEvent.rideId, startEvent.time, ride.time, ride.travelDistance))
          case _ => // we have no start record, ignore this one
        }
      }
    }

  }

  /**
   * Computes the average speed per taxi trip. It emits the rideId and average speed as a tuple.
   */
  class SpeedComputer extends MapFunction[TaxiTrip, (Long, Float)] {
    override def map(trip: TaxiTrip): (Long, Float) = {
      val startTime = trip.startTime.getMillis
      val endTime = trip.endTime.getMillis
      val distance = trip.travelDistance

      val duration = endTime - startTime
      val avgSpeed = (distance / duration) * 1000 * 60 * 60

      (trip.id, avgSpeed)
    }
  }

}
