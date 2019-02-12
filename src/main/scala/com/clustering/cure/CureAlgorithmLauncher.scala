package com.clustering.cure

import java.io.PrintWriter
import java.util.Date

import com.clustering.cure.CureAlgorithmLauncher.ParseArgs.OptionMap
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object CureAlgorithmLauncher {

  def main(args: Array[String]): Unit = {

    val cureArgs:OptionMap = ParseArgs.parse(args)

    val cureParams = CureArgs(confidence = cureArgs.getOrElse('confidence, 0.99d).toString.toDouble,
      clusters = cureArgs.getOrElse('clusters, throw new RuntimeException(s"no --clusters specified")).toString.toInt,
      representatives = cureArgs.getOrElse('representatives, throw new RuntimeException(s"no --representatives specified")).toString.toInt,
      shrinkingFactor = cureArgs.getOrElse('shrinkingFactor, throw new RuntimeException(s"no --shrinkingFactor specified")).toString.toDouble,
      partitions = cureArgs.getOrElse('partitions, throw new RuntimeException(s"no --partitions specified")).toString.toInt,
      inputFile = cureArgs.getOrElse('inputFile, throw new RuntimeException(s"no --inputFile specified")).toString,
      outputFile = cureArgs.getOrElse('outputFile, throw new RuntimeException(s"no --outputFile specified")).toString,
      samplingRatio = cureArgs.getOrElse('samplingRatio, 1.0d ).toString.toDouble,
      removeOutliers = cureArgs.getOrElse('removeOutliers, "false").toString.toBoolean)
    val startTime = System.currentTimeMillis()

    val result = CureAlgorithm.start(cureParams) // Launch cure algorithm

    val resultFile = cureParams.outputFile + "_" + new Date().getTime.toString
    result.saveAsTextFile(resultFile)
    val endTime = System.currentTimeMillis()
    val conf = new Configuration()
    val fs= FileSystem.get(conf)
    val output = fs.create(new Path(s"$resultFile/runtime.txt"))
    val writer = new PrintWriter(output)
    val text = s"Total time taken to assign clusters is : ${((endTime - startTime)*1.0)/1000} seconds"
    println(text)
    try {
      writer.write(text)
      writer.write(System.lineSeparator())
    }
    finally {
      writer.close()
    }
    print(System.lineSeparator() + "Job Completed Successfully")

  }
  object ParseArgs extends Serializable {

    type OptionMap = Map[Symbol, Any]
    private val usage = s"Using default command-line options. Possible options are: [--inputFile String] [--outputFile String] [--partitions Int] [--representatives Int]" +
      s" [--shrinkingFactor Double] [--clusters Int] [--sampling True/False] "

    def parse(args: Array[String]): OptionMap = {
      // Supply empty args if args is null.
      val copyArgs = if (args == null) {
        Array[String]()
      } else {
        args
      }
      // Provide usage suggestion if no args were supplied.
      if (copyArgs.length == 0) {
        println(usage)
      }
      val arglist = copyArgs.toList

      def nextOption(map: OptionMap, list: List[String]): OptionMap = {
        def isSwitch(s: String) = s(0) == '-'
        list match {
          case Nil => map
          case "--confidence" :: value :: tail =>
            nextOption(map ++ Map('confidence -> value), tail)
          case "--clusters" :: value :: tail =>
            nextOption(map ++ Map('clusters -> value), tail)
          case "--representatives" :: value :: tail =>
            nextOption(map ++ Map('representatives -> value), tail)
          case "--shrinkingFactor" :: value :: tail =>
            nextOption(map ++ Map('shrinkingFactor -> value), tail)
          case "--partitions" :: value :: tail =>
            nextOption(map ++ Map('partitions -> value), tail)
          case "--samplingRatio" :: value :: tail =>
            nextOption(map ++ Map('samplingRatio -> value), tail)
          case "--removeOutliers" :: value :: tail =>
            nextOption(map ++ Map('removeOutliers -> value), tail)
          case "--inputFile" :: value :: tail =>
            nextOption(map ++ Map('inputFile -> value), tail)
          case "--outputFile" :: value :: tail =>
            nextOption(map ++ Map('outputFile -> value), tail)
          case option :: tail => throw new IllegalArgumentException(s"unknown option: $option")
        }
      }
      nextOption(Map(), arglist)
    }
  }
}


case class CureArgs(confidence:Double,
                    clusters: Int,
                    representatives:Int,
                    shrinkingFactor:Double,
                    partitions:Int,
                    inputFile :String,
                    outputFile: String,
                    samplingRatio: Double = 1.0d,
                    removeOutliers : Boolean = false)
