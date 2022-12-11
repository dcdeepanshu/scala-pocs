package datastreams

import org.apache.flink.streaming.api.scala._


object EssentialStreams {

  def applicationTemplate(): Unit = {
    /*
    * ENTRY POINT of any flink app - execution environment
    * getExecutionEnvironment() - Factory method
    */
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //in between, we will have any sort of computation
    // this is computed in parallel so will be out of order
    val numberStream: DataStream[Int] = env.fromElements(1,2,3,4)
    //perform some action
    numberStream.print()


    //end - trigger all the computations
    env.execute()
  }


  def main(args: Array[String]): Unit = {
    applicationTemplate()
  }

}
