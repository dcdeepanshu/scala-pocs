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


  //transformations
  def demoTransformations(): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val numbers: DataStream[Int] = env.fromElements(1,2,3,4)

    //check parallelism/no of cores of machine
    println(s"Current parallelism/cores: ${env.getParallelism}")

    env.setParallelism(2)

    //map
    val doubledNumbers: DataStream[Int] = numbers.map(_ * 2)

    //flat map
    val expandedNumbers: DataStream[Int] = numbers
      .flatMap(n => List(n, n + 1))
      .setParallelism(1) //set parallelism for this specific transformation

    //filter
    val filteredEvenNumbers: DataStream[Int] = numbers.filter(_ % 2 == 0)


    expandedNumbers.print()
    //expandedNumbers.writeAsText("output/expandedStream")

    env.execute()
  }

  def fizzBuzz(n: Int): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val numbers = env.fromSequence(1, 100)


    val fizzBuzzed = numbers
      .map((n) => {
        val isDivisbleByThree: Boolean =  n % 3 == 0
        val isDivisbleByFive: Boolean =  n % 5 == 0
        if (isDivisbleByThree && isDivisbleByFive) (n, "fizzbuzz")
        else if (isDivisbleByThree) (n, "fizz")
        else if (isDivisbleByFive) (n, "buzz")
        else (n, s"$n")
      })

    fizzBuzzed.print()

    env.execute()
  }

  def main(args: Array[String]): Unit = {
    fizzBuzz(50)
  }

}
