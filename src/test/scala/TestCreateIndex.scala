import PartitionN.partionne_en_N_Rec
import Index.{declPos, file_to_RDD, raPos}
import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FunSuite}

class TestCreateIndex extends FunSuite with BeforeAndAfter {

  var sc: SparkContext = null;

  // exécuté avant chaque test
  before {
    // configuration de Spark
    val conf = new SparkConf()
      .setAppName("SparkTestIndex")
      .setMaster("local") // ici on configure un faux cluster spark local pour le test
    sc = new SparkContext(conf)
  }

  test("Test create Index") {
    val srcFile = "samples/source-sample"
    val destIndex = "index/"
    val conf = new SparkConf().setAppName("createIndex-Test")
    val baseSet = Index.file_to_RDD(srcFile, declPos, sc)
    val res = partionne_en_N_Rec(baseSet, 10, sc)
    var i = 0;
    val df = sc.textFile("samples/*").foreach(e => println(e))

    res.foreach(partition => {
      partionne_en_N_Rec(partition.map(el => (el._2(raPos).toDouble, el._2)), 10, sc)
        .foreach(subPartition => {
          subPartition.map(el => el._2.mkString(",")).saveAsTextFile(destIndex+"partition_"+i)
          i = i+1
        })
    })

    assert(1 == 1, ", wrong number of total elements")
  }

  // exécuté après chaque test
  after {
    sc.stop() // a ne pas oublier, car il ne peut pas y avoir 2 contextes spark locaux simultanément
  }
}
