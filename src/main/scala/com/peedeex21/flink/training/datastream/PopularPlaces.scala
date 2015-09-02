package com.peedeex21.flink.training.datastream


import java.util.concurrent.TimeUnit

import com.dataArtisans.flinkTraining.exercises.dataStreamJava.dataTypes.TaxiRide
import com.dataArtisans.flinkTraining.exercises.dataStreamJava.utils.{GeoUtils, TaxiRideGenerator}
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.functions.WindowMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.helper.Time
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._

/**
 * The task of the “Popular Places” exercise is to identify popular places and events from the taxi ride data
 * stream. This is done by counting every five minutes the number of taxi rides that started and ended in the same
 * area within the last 15 minutes. Arrival and departure locations should be separately counted. Only locations with
 * more arrivals or departures than a provided popularity threshold should be forwarded to the stream.
 */
object PopularPlaces {

  // program constants
  private val WINDOW_LENGTH = 15 * 60 * 1000L // 15 minutes in msecs
  private val EVICTION_FREQUENCY = 5 * 60 * 1000L // 5 minutes in msecs

  // program arguments
  private var inputFile: Option[String] = None
  private var servingSpeedFactor: Option[Float] = None
  private var popularityIndex: Option[Int] = None


  /**
   * Runs the PopularPlaces program.
   *
   * @param args inputFile as String, servingSpeedFactor as Float, popularityIndex as Int
   */
  def main(args: Array[String]) {

    // read the program arguments
    if (!parseArguments(args)) {
      return
    }

    // set up the streaming execution environment
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment

    // adjust window size and eviction interval to fast-forward factor
    val windowSize = (WINDOW_LENGTH / servingSpeedFactor.get).toLong
    val evictionInterval = (EVICTION_FREQUENCY / servingSpeedFactor.get).toLong

    // add a source, taxi rides are read from file and streamed
    val rides = streamEnv.addSource(new TaxiRideGenerator(inputFile.get, servingSpeedFactor.get))

    // find places where n taxis start / stop
    val popularPlaces = rides.filter(ride => {
        GeoUtils.isInNYC(ride.startLon, ride.startLat) && GeoUtils.isInNYC(ride.endLon, ride.endLat)
      })
      .map(new GirdCellMapper)
      .groupBy(0, 1)
      .window(Time.of(windowSize, TimeUnit.MILLISECONDS))
      .every(Time.of(evictionInterval, TimeUnit.MILLISECONDS))
      .mapWindow(new PopularityCounter(popularityIndex.get))
      .flatten()
      .map(new GridToCoordinates)

    // emit longitude, latitude, isStart, popularityCounter on stdout
    popularPlaces.print()

    // run the program
    streamEnv.execute("Popular Places")
  }

  /**
   * Checks and parses the program arguments.
   *
   * @param args inputFile as String, servingSpeedFactor as Float, popularityIndex as Int
   * @return true of right parameter are given, otherwise false
   */
  private def parseArguments(args: Array[String]): Boolean = {
    if (args.length != 3) {
      // there must be 4 arguments given
      printArgumentError()
      false
    } else {
      // parse the arguments
      try {
        inputFile = Some(args(0))
        servingSpeedFactor = Option(args(1).toFloat)
        popularityIndex = Option(args(2).toInt)
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
      |   Usage: TaxiRideCleansing <inputFile> <servingSeedFactor> <popularityIndex>
    """.stripMargin)


  /**
   * Mapper for matching a [[TaxiRide]] event to a grid cell within NYC geo boundaries.
   */
  class GirdCellMapper extends MapFunction[TaxiRide, (Boolean, Int)] {
    override def map(ride: TaxiRide): (Boolean, Int) = {
      if (ride.isStart) {
        (ride.isStart, GeoUtils.mapToGridCell(ride.startLon, ride.startLat))
      } else {
        (ride.isStart, GeoUtils.mapToGridCell(ride.endLon, ride.endLat))
      }
    }

  }

  /**
   * Counts how many taxi rides start / stop in a grid cell within the current window. Counts not exceeding the
   * provided threshold.
   */
  class PopularityCounter(popularityIndex: Int) extends WindowMapFunction[(Boolean, Int), (Boolean, Int, Int)] {
    override def mapWindow(input: java.lang.Iterable[(Boolean, Int)], collector : Collector[(Boolean, Int, Int)])
        : Unit = {
      // count records in window and build output record
      val result = input.asScala.foldLeft((false, 0, 0)) { (l, r) => (r._1, r._2, l._3 + 1) }

      // check threshold
      if (result._3 > popularityIndex) {
        // emit record
        collector.collect(result)
      }
    }
  }

  /**
   * Maps a gird cell id to a longitude / latitude pair.
   */
  class GridToCoordinates extends MapFunction[(Boolean, Int, Int), (Float, Float, Boolean, Int)] {
    override def map(input: (Boolean, Int, Int)): (Float, Float, Boolean, Int) = {
      val lon = GeoUtils.getGridCellCenterLon(input._2)
      val lat = GeoUtils.getGridCellCenterLat(input._2)

      (lon, lat, input._1, input._3)
    }
  }

}
