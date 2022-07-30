import PartitionDeux.{file_to_RDD, partionne_en_deux}
import PartitionN.partionne_en_N_Rec
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FunSuite}

class TestPartitionN extends FunSuite with BeforeAndAfter {

  var sc: SparkContext = null;
  var N = 10
  // exécuté avant chaque test
  before {
    // configuration de Spark
    val conf = new SparkConf()
      .setAppName("SampleSparkTest2")
      .setMaster("local") // ici on configure un faux cluster spark local pour le test
    sc = new SparkContext(conf)
  }

  test("sample spark test 2") {
    val srcFile = "samples/source-sample"
    val baseSet = file_to_RDD(srcFile, sc)
    val baseSetSize = baseSet.count()
    val nbElementPerPartition = baseSet.count() / N
    val res = partionne_en_N_Rec(baseSet, N, sc)
    println("taille total " + res.length)
    assert( res.length == N, ", wrong number partition")
    var totalElem = 0L
    for (c <- res){
      println("taille part " + c.count())
      totalElem += c.count()
     // assert( c.count() == nbElementPerPartition || c.count() == nbElementPerPartition + 1, ", wrong number of elements in a partition")
    }
    assert( totalElem == baseSetSize, ", wrong number of total elements")
  }

  // exécuté après chaque test
  after {
    sc.stop() // a ne pas oublier, car il ne peut pas y avoir 2 contextes spark locaux simultanément
  }
}