import spark.SparkContext
import SparkContext._

object SimpleJob {
    def main(args: Array[String]) 
   {
    val logFile = "/home/hadoop/hadoop/logs/hadoop-hadoop-tasktracker-master.log.2013-05-22" // Should be some file on your system
    val sc = new SparkContext("local", "Simple Job", "/home/hadoop/amplab/spark-0.7.2",List("/home/hadoop/amplab/spark-0.7.2/codes/SimpleJob/target/scala_2.9.3/simplejob_2.9.3-1.0.jar"))
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  }
}
