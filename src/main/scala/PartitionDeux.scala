import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scala.math.Numeric.DoubleIsFractional

object PartitionDeux {
  val compte = "baptFlo"
  val palierTraitementNonRecurs = 500
  val declPos = 9

  def partionne_en_deux_Rec(rdd: RDD[(Double, Array[String])], nb_X: Long, nb_Y: Long, sc: SparkContext): (RDD[(Double, Array[String])], RDD[(Double, Array[String])]) = {
    rdd.cache()
    if (rdd.count() < palierTraitementNonRecurs) { // Traitement non récursif car la taille est assez petite
      val res = rdd
        .collect()
        .sortBy(el => el._1)
      (sc.parallelize(res.slice(0, nb_X.toInt)), sc.parallelize(res.slice(nb_X.toInt, nb_X.toInt + nb_Y.toInt)))
    } else {

      // On tire un pivot aléatoirement
      val pivot = rdd.takeSample(false, 1)(0)._1

      // On découpe le RDD en :
      // - A : contient les éléments < pivot
      // - B : contient les éléments >= pivot

      val A = rdd.filter(el => el._1 < pivot)
      val B = rdd.filter(el => el._1 >= pivot)
      // val C = rdd.filter(el => el._1 == pivot)


      val sizeA = A.count()
      val sizeB = B.count()
      if (sizeA < nb_X) {
        // Trop d'élément dans la partie B
        // On update nb_X et nb_Y
        val tmp = nb_X - sizeA
        val res = partionne_en_deux_Rec(B, tmp, sizeB - tmp, sc)

        (A ++ res._1, res._2)
      } else if (sizeA > nb_X) {
        // Trop d'élément dans la partie A
        // On update nb_X et nb_Y
        val tmp = nb_Y - sizeB
        val res = partionne_en_deux_Rec(A, sizeA - tmp, tmp, sc)

        (res._1, B ++ res._2)
      } else {
        // Taille égale, c'est gagné
        (A, B)
      }
    }

  }

  def file_to_RDD(src: String, sc: SparkContext): RDD[(Double, Array[String])] = {
    sc.textFile(src)
      .map(_.split(",").map(_.trim))
      .map(el => (el(declPos).toDouble, el))
  }

  def partionne_en_deux(rdd: RDD[(Double, Array[String])], sc: SparkContext): (RDD[(Double, Array[String])], RDD[(Double, Array[String])]) = {
    val size = rdd.count() / 2
    val size2 = if (size % 2 == 0) size else size + 1 // Au cas ou la taille est impair
    partionne_en_deux_Rec(rdd, size, size2, sc)
  }


  def main(args: Array[String]): Unit = {
    if (args.length == 1) {
      // val srcFile = "/tp-data/Source/Source-001.csv"
      val conf = new SparkConf().setAppName("Partition2-" + compte)
      val sc = new SparkContext(conf)
      val baseSet = file_to_RDD(args(0), sc)
      val res = partionne_en_deux(baseSet, sc)
      println("Size part 1 : " + res._1.count() + " // Size part 2 : " + res._2.count())
    } else {
      println("Usage: spark-submit --class SparkTPApp3 /home/\" + compte + \"/SparkTPApp-assembly-1.0.jar path/to/sources")
    }
  }
}
