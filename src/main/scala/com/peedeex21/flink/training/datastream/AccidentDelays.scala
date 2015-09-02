package com.peedeex21.flink.training.datastream

import com.dataArtisans.flinkTraining.exercises.dataStreamJava.dataTypes.{Accident, TaxiRide}
import com.dataArtisans.flinkTraining.exercises.dataStreamJava.utils.{AccidentGenerator, GeoUtils, TaxiRideGenerator}
import org.apache.flink.api.common.functions.{FlatMapFunction, MapFunction}
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * The task of the “Taxi Ride Cleansing” exercise is to cleanse a stream of TaxiRide records by removing records that
 * do not start or end in New York City. Erroneous records with invalid start and end location coordinates are
 * removed by this check as well. The cleansed TaxiRide stream should be written to Apache Kafka.
 */
object AccidentDelays {

  // program arguments
  private var inputFile: Option[String] = None
  private var servingSpeedFactor: Option[Float] = None

  /**
   * Runs the TaxiRideCleansing program.
   *
   * @param args inputFile as String, servingSpeedFactor as Float
   */
  def main(args: Array[String]) {

    // read the program arguments
    if (!parseArguments(args)) {
      return
    }

    // set up the streaming execution environment
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment

    // open stream for taxi rides and map them to all crossing grid cells
    val rides = streamEnv.addSource(new TaxiRideGenerator(inputFile.get, servingSpeedFactor.get))
      .filter(ride => {
        GeoUtils.isInNYC(ride.startLon, ride.startLat) && GeoUtils.isInNYC(ride.endLon, ride.endLat)
      })
      .flatMap(new TripGridCellMapper)
      .partitionByHash(0)

    // open the stream for accidents, and map them to a grid cell
    val accidents = streamEnv.addSource(new AccidentGenerator(servingSpeedFactor.get))
      .map(new AccidentGridCellMapper)
      .partitionByHash(0)

    // join rides and accidents by cell and emit message if they concern each other
    val delayedRides = rides
      .connect(accidents)
      .flatMap(new AccidentRideConnector)

    // emit the result
    delayedRides.print

    // execute the program
    streamEnv.execute("Accident Delays")
  }

  /**
   * Checks and parses the program arguments.
   *
   * @param args inputFile as String, servingSpeedFactor as Float
   * @return true of right parameter are given, otherwise false
   */
  private def parseArguments(args: Array[String]): Boolean = {
    if (args.length != 2) {
      // there must be 4 arguments given
      printArgumentError()
      false
    } else {
      // parse the arguments
      try {
        inputFile = Some(args(0))
        servingSpeedFactor = Option(args(1).toFloat)
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

  /**
   * Maps a accident to the gird cell where it occurred.
   */
  class AccidentGridCellMapper extends MapFunction[Accident, (Int, Accident)] {
    def map(accident: Accident): (Int, Accident) = {
      val gridCell = GeoUtils.mapToGridCell(accident.lon, accident.lat)
      (gridCell, accident)
    }
  }

  /**
   * Maps a taxi ride to all crossed grid cells depending on their start and end location.
   */
  class TripGridCellMapper extends FlatMapFunction[TaxiRide, (Int, TaxiRide)] {
    override def flatMap(ride: TaxiRide, collector: Collector[(Int, TaxiRide)]): Unit = {
      val tripGridCellIds =
        GeoUtils.mapToGridCellsOnWay(ride.startLon, ride.startLat, ride.endLon, ride.endLat).asScala

      tripGridCellIds.foreach(id => collector.collect((id, ride)))
    }
  }

  /**
   * Emits a message if a accident occurs in a cell, where an ongoing taxi rides crosses. Taxi rides and accidents
   * are cached during their duration.
   */
  class AccidentRideConnector extends CoFlatMapFunction[(Int, TaxiRide), (Int, Accident), (Int, TaxiRide)] {

    private val ongoingTaxiRides =  mutable.HashMap.empty[Int, mutable.Set[TaxiRide]]
    private val ongoingAccidents = mutable.HashMap.empty[Int, mutable.Set[Accident]]

    /**
     * Remembers starting taxi rides, forgets ending ones.
     * If ride starts in affected cell, a message is emitted.
     *
     * @param input tuple of gird id, taxi ride event
     * @param out message if ride starts in same cell where accident is ongoing
     */
    override def flatMap1(input: (Int, TaxiRide), out: Collector[(Int, TaxiRide)]): Unit = {
      val cellId = input._1
      val ride = input._2

      if (ride.isStart) {
        // taxi ride starts, remember the ride
        val rideSet = ongoingTaxiRides.getOrElseUpdate(cellId, mutable.Set.empty[TaxiRide])
        rideSet.add(ride)

        // check if there is an accident
        val accidents = ongoingAccidents.getOrElseUpdate(cellId, mutable.Set.empty[Accident])
        if (accidents.nonEmpty) {
          // there is an accident, emit a message
          out.collect(input)
        }

      } else {
        // taxi ride end, forget it
        ongoingTaxiRides.get(cellId) match {
          case Some(taxiRideSet) => taxiRideSet.remove(ride)
          case _ => // nothing to do
        }
      }
    }

    /**
     * Remembers incoming emerging accident, forgets cleared ones.
     * If its emerging am message for all rides, crossing this cell, is emitted.
     *
     * @param input tuple of grid id, accident event
     * @param out messages for affected accidents
     */
    override def flatMap2(input: (Int, Accident), out: Collector[(Int, TaxiRide)]): Unit = {
      val cellId = input._1
      val accident = input._2

      if(accident.isCleared) {
        // accident is cleared, forget it
        ongoingAccidents.get(cellId) match {
          case Some(set) => set.remove(accident)
          case _ => // nothing to do
        }
      } else {
        // accident emerges, remember it
        val accidentSet = ongoingAccidents.getOrElseUpdate(cellId, mutable.Set.empty[Accident])
        accidentSet.add(accident)

        // check if there is an ongoing ride in this cell
        ongoingTaxiRides.get(cellId) match {
          // emit a message for each ride
          case Some(rideSet) => rideSet.foreach(ride => out.collect((cellId, ride)))
          case _ => // nothing to do
        }
      }
    }
  }

}
