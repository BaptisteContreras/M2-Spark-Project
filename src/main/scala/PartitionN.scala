import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object PartitionN {
  val compte = "baptFlo"

  val declPos = 9

  def file_to_RDD(src: String, sc: SparkContext): RDD[(Double, Array[String])] = {
    sc.textFile(src)
      .map(_.split(",").map(_.trim))
      .map(el => (el(declPos).toDouble, el))
  }

  def partionne_en_N_Rec(rdd: RDD[(Double, Array[String])], N: Long, sc: SparkContext): Array[RDD[(Double, Array[String])]] = {
    rdd.cache()
    if (N >= 2) { // Si N est assez grand pour être partitionné
      if (N % 2 == 0) {
        // Cas ou N est pair, on divise en deux parties égales et on redivise chacune des deux nouvelles partitions en N/2
        val tmp = PartitionDeux.partionne_en_deux(rdd, sc)
        val nDivided = N / 2
        partionne_en_N_Rec(tmp._1, nDivided, sc) ++ partionne_en_N_Rec(tmp._2, nDivided, sc)
      } else {
        // Dans le cas ou N est impair, on découpe en deux partie, avec une plus grand et une plus petite, puis on redivise
        // la plus grande partition en N-1, avec N-1 pair
        // exemple, pour N = 5, on fait une partition avec 1/5 des éléments et une autre de 4/5 et on redivise celle de 4/5 en 4 parties
        val rddSize = rdd.count()
        val sizeFirstPartition = rddSize / N
        val sizeOtherPartition = (N - 1) * sizeFirstPartition
        val res1 = PartitionDeux.partionne_en_deux_Rec(rdd, sizeFirstPartition, sizeOtherPartition, sc)

        Array(res1._1) ++ partionne_en_N_Rec(res1._2, N - 1, sc)
      }
    } else {
      Array(rdd)
    }
  }


  def main(args: Array[String]): Unit = {
    if (args.length == 2) {
      // val srcFile = "/tp-data/Source/Source-001.csv"
      val conf = new SparkConf().setAppName("partitionN-" + compte)
      val sc = new SparkContext(conf)
      val baseSet = file_to_RDD(args(0), sc)
      val res = partionne_en_N_Rec(baseSet, args(1).toInt, sc)

      println("taille total " + res.length)
    } else {
      println("Usage: spark-submit --class SparkTPApp3 /home/\" + compte + \"/SparkTPApp-assembly-1.0.jar path/to/sources nbPartition")
    }

  }
}
