import java.io.{BufferedWriter, File, FileWriter}

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
/**
  * Created by Mayanka on 19-06-2017.
  */
object NGRAM {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local[*]")
      .set("spark.driver.memory", "6g")
    val sc = new SparkContext(sparkConf)
    val input = sc.wholeTextFiles("C:\\kdm\\Tutorial 3 Source Code\\SparkOpenIE_WordNET_LDA\\data\\abstract_text", minPartitions = 10)
    val newfolder= new File("Ngram_output")
    val file = new File("C:\\kdm\\Spark_TFIDF_W2V\\Ngram_output\\output.txt")
    val bw = new BufferedWriter(new FileWriter(file))
    val files = input.map { case (filename, content) => filename}
    def processFile(file: String) = {
      //println(file)
      val logData = sc.textFile(file).collect();
      val str =logData.toString()

      for(i <- 0 until logData.length){
        val a = getNGrams( logData(i),3)
        if(newfolder.exists()){
        //val file_data=a.foreach(f=>println(f.mkString(" ")))

          a.foreach(f=>bw.append(f.mkString(" ")+"\n"))}
          //bw.close()}
      }

    }
    files.collect.foreach( filename => {
      processFile(filename)
    })

    //val a = getNGrams("the bee is the bee of the bees", 2)
    //a.foreach(f=>println(f.mkString(" ")))}
  }

  def getNGrams(sentence: String, n: Int): Array[Array[String]] = {
    val words = sentence
    val ngrams = words.split(' ').sliding(n)
    ngrams.toArray
   }
}



