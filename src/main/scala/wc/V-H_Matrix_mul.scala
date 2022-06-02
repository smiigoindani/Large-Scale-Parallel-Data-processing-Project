package vh

import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
//import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.sql.types.IntegerType


object VH_Matrix_Main {
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\n vh.VH_Matrix_Main <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Matrix Multiplication").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

		// Delete output directory, only to ease local development; will not work on AWS. ===========
//    val hadoopConf = new org.apache.hadoop.conf.Configuration
//    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
//    try { hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true) } catch { case _: Throwable => {} }
		// ================

    // input for left matrix
    val textfile1 = sc.textFile(args(0),100)
    val linesrdd1 = textfile1.map(line => {
                        val row = line.split(",")
                        (row(0).toInt,row(1).toInt,row(2).toInt)
                        })
	  
    // input for right matrix
    val textfile2 = sc.textFile(args(1),100)
    val linesrdd2 = textfile2.map(line => {
                        val row = line.split(",")
                        (row(0).toInt,row(1).toInt,row(2).toInt)
                        })
     
    //val linesrdd1 = sc.parallelize(Seq((0,0,1),(0,1,2),(1,1,4),(2,0,5),(2,1,6)))
   
    
    //val linesrdd2 = sc.parallelize(Seq((0,0,1),(0,1,2),(1,0,3),(1,1,4)))
  
    // emitting column for left matrix
    //val M = linesrdd1.map({ case (i, j, v) => (j, (i, v)) })
    // emitting row for right matrix
    //val N = linesrdd2.map({ case (j, k, w) => (j, (k, w)) })
    
    // joining on column = row
    val productEntries = M.join(N)
    .map({ case (_, ((i, v), (k, w))) => ((i, k), (v * w)) })
    .reduceByKey(_ + _)
    .map({ case ((i, k), sum) =>(i, k, sum) })
     // map the output as (row,column,value)

    productEntries.collect().foreach(println)
    productEntries.saveAsTextFile(args(2))
	  
    val endTimeMillis = System.currentTimeMillis()
    val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
    println("Execution Time :------------------"+ durationSeconds)

    
    
  }
}
