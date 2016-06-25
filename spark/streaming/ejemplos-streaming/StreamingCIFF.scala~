import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by chicochica10 on 8/07/15.
  */
object StreamingUTAD {
  def main (args: Array[String]): Unit ={

    val inputFile = args(0)
    val outputFile = args(1)

    val conf = new SparkConf().setAppName("StreamingUTAD")
    val ssc = new StreamingContext(conf, Seconds(1)) //<@@@ batch interval
    val lines = ssc.socketTextStream("127.0.0.1",7777)
    //val lines = ssc.textFileStream("/tmp/logdata")

    //filtrando por error
    //val errorLines = lines.filter (_.contains("error"))
    //errorLines.print()
    //errorLines.saveAsTextFiles(s"${outputFile}errorLines.txt")

    //contando por ip's en cada paso de tiempo
    val accessLogDStream = lines.map (line => ApacheAccessLog.parseFromLogLine(line))
    val ipDStream = accessLogDStream.map (entry => (entry.getIpAddress, 1))
    val ipCountsDStream = ipDStream.reduceByKey((x,y) => x + y)
    //println ("@@@ ip's en cada batch ")
    // ipCountsDStream.print()
    // ipCountsDStream.saveAsTextFiles(s"${outputFile}ipCounts.txt")
    //println ("@@@ end ip's en cada batch ")
    /*
    Time: 1436367773000 ms
    (66.249.69.97,1)
    (71.19.157.174,4)


    Time: 1436367778000 ms
    (71.19.157.174,1)

    Time: 1436367779000 ms
    (66.249.69.97,1)
    (71.19.157.174,4)

    Time: 1436367781000 ms
    (66.249.69.97,1)
    (71.19.157.174,4)
     */

   // join two dstreams
    // val ipBytesDStream = accessLogDStream.map (entry => (entry.getIpAddress, entry.getContentSize))
    // val ipBytesSumDStream = ipBytesDStream.reduceByKey( (x,y) => x +y)

   // val ipBytesRequestCountDStream = ipCountsDStream.join (ipBytesSumDStream) // cuenta y peso de las peticiones por ip's en cada slice
    //println ("@@@ cuenta y peso de las peticiones por ip's en cada slice")
    //ipBytesRequestCountDStream.print()
    //ipBytesRequestCountDStream.saveAsTextFiles(s"${outputFile}ipBytesRequestCount.txt")
    //println ("@@@ end cuenta y peso de las peticiones por ip's en cada slice")
    /*
    (66.249.69.97,(1,514))
    (71.19.157.174,(4,21225))
     */

    // transform:
    /*
    A common application of transform() is to
    reuse batch processing code you had written on RDDs. For example, if you had a
    function, extractOutliers() , that acted on an RDD of log lines to produce an RDD
    of outliers (perhaps after running some statistics on the messages), you could reuse it
     */
    //val outlierDStream = accessLogDStream.transform { rdd =>
    //  extractOutliers(rdd)
    //}

    ////////////////////////////////////////
    //stateful operations
    ssc.checkpoint("./checkpoint") //<- needed for stateful operations (1.- windowed operations & 2.-updateStateByKey)

    //1.- windowed operations
    // windowDuration
    // sliding duration

    //multiplos de batchInterval
    //bi -> 10 sec. para 3 batches -> windowDuration-> 30 sec (ponemos window duration a 30 para pillar 3 batches)
    //slidingDuration -> defecto batch interval nos dice con que frecuencia  se computa un nuevo dstream
    //bi -> 10 sec. para computar en el segudo batch -> sliding duration -> 20 sec

    /*batch interval 1 sec
    A windowed stream with a window duration of 3 batches and a slide
    duration of 2 batches (3 en el ejemplo); every two time steps, we compute a result over the previous 3
    time steps
     */

    //val accessLogsWindow = accessLogDStream.window(Seconds (3), Seconds(3))

    //val windowCounts = accessLogsWindow.count()
   // println("@@@ windows count")
   //  windowCounts.saveAsTextFiles(s"${outputFile}windowscount.txt")
   // println("@@@ end windows count")


    //reduceByWindow & reduceByKeyAndWindow
    //val ipCountsDStreamWindow = ipDStream.reduceByKeyAndWindow(
    //  (x,y) => x + y ,
    //  (x,y) => x - y,
    //  Seconds (3),
    //  Seconds (2)
    //)
    //println("@@@ count by ip and window")
    //ipCountsDStreamWindow.saveAsTextFiles(s"${outputFile}reduceByKeyAndWindow.txt")
    //println("@@@ end count by ip and window")

    // countByWindow & countByValueAndWindow
    //val requestCount = accessLogDStream.countByWindow (Seconds(3),Seconds(2))
    // requestCount.saveAsTextFiles(s"${outputFile}countByWindow.txt") // deberia ser los mismo que windowCounts

    //val ipAddresRequestCount = accessLogDStream.map{entry => entry.getIpAddress()}.countByValueAndWindow(Seconds(3),Seconds(1))
    // ipAddresRequestCount.saveAsTextFiles(s"${outputFile}countByValueAndWindow.txt") // deberia ser lo mismo que reduceByKeyAndWindow

    //2.- updateStatetByKey
    // values: list of events that arrived in current batch (may be empty)
    // oldState: optional state object stored within an Option, it may be missing if no previous state for the key
    def updateRunningSum(values: Seq[Long], oldState: Option[Long]) = {
      //println ("@@@ values: " + values)
      //println ("@@@ state: " + oldState)
      val sgoe = oldState.getOrElse(0L)
      val vs = values.size
      //println ("@@@ state.getOrElse: " + sgoe)
      //println ("@@@ values.size: " + values.size)
      Some(sgoe + vs)

      /*
      values: CompactBuffer(1, 1, 1, 1, 1, 1, 1, ...)
      @@@ oldState: Some(8662)
      @@@ oldState.getOrElse: 8662
      @@@ values.size: 353009
      @@@ values: CompactBuffer(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, ... ) mas QUE VALUES
       */
    }

    val responseCodeDStream = accessLogDStream.map (entry => (entry.getResponseCode, 1L))
    val responseCodeCountDStream = responseCodeDStream.updateStateByKey(updateRunningSum)

     responseCodeCountDStream.saveAsTextFiles(s"${outputFile}updateStateByKey.txt")


    // Sequence files
  /*  val writableIpAddressRequestCount = ipBytesSumDStream.map {
      case (ip, count) => (new Text(ip), new LongWritable(count)) }

    writableIpAddressRequestCount.saveAsHadoopFiles[
      SequenceFileOutputFormat[Text, LongWritable]]("outputDir", "txt")

  */

    ssc.start() //in separate thread
    ssc.awaitTermination()
    // nc -lk 7777 <-- net cat por consola


    /*
    Windows users can use the ncat command in place of the nc command. ncat is
available as part of nmap .
     */

      // para lanzar pseudologs
    //./fakelogs.sh

     //UI de spark mientras está corriendo
    //localhost:4040/stages/

  }
}
