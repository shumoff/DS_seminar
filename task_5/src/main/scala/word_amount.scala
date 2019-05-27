import org.apache.spark.{SparkConf, SparkContext}

object word_amount {
  def main(args: Array[String]): Unit =
  {
    val conf = new SparkConf().setAppName("word_amount").setMaster("local")
    val sc = new SparkContext(conf)

    val text_1 = sc.textFile("C:\\Users\\dmitr\\PycharmProjects\\DS_seminar\\task_5\\data\\text_1.txt")
    val text_2 = sc.textFile("C:\\Users\\dmitr\\PycharmProjects\\DS_seminar\\task_5\\data\\text_2.txt")

    val regex = """[^a-zA-Z ]""".r

    val words_text_1 = text_1.flatMap(line=>regex.replaceAllIn(line, "").toLowerCase().split(" ")).collect()
    val words_text_2 = text_2.flatMap(line=>regex.replaceAllIn(line, "").toLowerCase().split(" "))

    val text_2_all_words_amount = words_text_2.map(word=>(word, 1)).reduceByKey(_+_).collect()

    val words_text_2_from_text_1 = words_text_2.filter(word=>words_text_1.contains(word))
      .map(word=>(word, 1)).reduceByKey(_+_).collect()
    
    println("Amounts of words in text_2:")
    text_2_all_words_amount.foreach(println)
    println("Amounts of words in text_1 that are also in text_2:")
    words_text_2_from_text_1.foreach(println)
  }
}