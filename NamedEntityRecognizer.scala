import java.io.{BufferedWriter, File, FileWriter}
import java.util.Properties

import edu.stanford.nlp.ling.CoreAnnotations._
import edu.stanford.nlp.ling.CoreLabel
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import edu.stanford.nlp.util.CoreMap
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConverters._


object NamedEntityRecognizer {

  var count = 0
  var count1 = 0
  var count2 = 0
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "c:\\winutils");


    val sparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    val inputf = sc.wholeTextFiles("C:\\kdm\\Tutorial 3 Source Code\\SparkOpenIE_WordNET_LDA\\data\\abstract_text", minPartitions = 10)


    val newfile = new File("newfile")
    val file = new File("C:\\kdm\\CoreNLP SourceCode\\CoreNLP\\newfile\\output.txt")
    val bw = new BufferedWriter(new FileWriter(file))
    val files = inputf.map { case (filename, content) => filename }

    def processFile(file: String) = {
      //println(file)
      val logData = sc.textFile(file).collect();
      val str = logData.toString()

      for (i <- 0 until logData.length) {
        var a = CoreNLP(logData(i))
        if (newfile.exists()) {
          //val file_data=a.foreach(f=>println(f.mkString(" ")))
          //println(a)
          //bw.append(a.toString)

        }
        //bw.close()}
      }

    }

    files.collect.foreach(filename => {
      processFile(filename)
    })
    println(count)
    println(count1)
    println(count2)
  }

    //val a = getNGrams("the bee is the bee of the bees", 2)
    //a.foreach(f=>println(f.mkString(" ")))}



  //  some text from a file
  // val inputFile: File = new File("C:\\kdm\\CoreNLP SourceCode\\CoreNLP\\src\\abstracts_1.txt")
  /*val wc=inputf.map(f => {
     val s = f._2.toString
     val splitString = f._2.split(" ")
     splitString.toSeq})*/

  //val text = "Hi i am sai sri. she is maynka"

  //  blank annotator

  def CoreNLP(text:String ) {

    val file = new File("C:\\kdm\\CoreNLP SourceCode\\CoreNLP\\newfile\\output.txt")
    val bw = new BufferedWriter(new FileWriter(file))
    val props: Properties = new Properties()
    props.put("annotators", "tokenize, ssplit, pos, lemma, ner")
    val pipeline: StanfordCoreNLP = new StanfordCoreNLP(props)
    // running all Annotator - Tokenizer on this text
    val document: Annotation = new Annotation(text)
    pipeline.annotate(document)

    val sentences: List[CoreMap] = document.get(classOf[SentencesAnnotation]).asScala.toList

    (for {
      sentence: CoreMap <- sentences
      token: CoreLabel <- sentence.get(classOf[TokensAnnotation]).asScala.toList
      word: String = token.get(classOf[TextAnnotation])
      pos: String = token.get(classOf[PartOfSpeechAnnotation])
      lemma: String = token.get(classOf[LemmaAnnotation])
      ner: String = token.get(classOf[NamedEntityTagAnnotation])


    } yield (token, word, pos, lemma, ner)) foreach {t=>
      if (t._3 == "NN" || t._3 == "NNS" || t._3 == "NNP" || t._3 == "NNPS") { count+=1
        println("word_nouns",t._2)}
      if (t._3 == "VB" || t._3 == "VBD" || t._3 == "VBG" || t._3 == "VBN" || t._3 == "VBP" || t._3 == "VBZ"){ count1+=1
      println("word_verbs", t._2)}
    /*if (t._5 != ' '){ count2+=1
      println ("word_ner", t._5)}*/

    }
  }
}


