import PartitionDeux.{file_to_RDD, partionne_en_deux}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FunSuite}

class TestPartition2 extends FunSuite with BeforeAndAfter {

  var sc: SparkContext = null;

  // exécuté avant chaque test
  before {
    // configuration de Spark
    val conf = new SparkConf()
      .setAppName("SampleSparkTest")
      .setMaster("local") // ici on configure un faux cluster spark local pour le test
    sc = new SparkContext(conf)
  }

  test("sample spark test") {
    val srcFile = "samples/source-sample"
    val baseSet = file_to_RDD(srcFile, sc)
    val res = partionne_en_deux(baseSet, sc)
    println("Size part 1 : " + res._1.count() + " // Size part 2 : " + res._2.count() )
    assert(res._1.count() == res._2.count(), ", wrong number elements")
  }

  // exécuté après chaque test
  after {
    sc.stop() // a ne pas oublier, car il ne peut pas y avoir 2 contextes spark locaux simultanément
  }
}
