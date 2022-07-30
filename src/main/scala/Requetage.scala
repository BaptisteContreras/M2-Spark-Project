import Requetage.loadPartition
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object Requetage {
  val compte = "baptFlo"

  // partition, minDecl, maxDecl, minRa, maxRa
  val minDeclPos = 1
  val maxDeclPos = 2
  val minRaPos = 3
  val maxRaPos = 4

  val declPos = 9
  val raPos = 6


  /**
   * Retourne un Tuple de coordonées
   */
  def coordTuple(minDecl: Double, maxDecl: Double, minRa: Double, maxRa: Double): ((Double, Double), (Double, Double)) = {
    ((minDecl, maxRa), (maxDecl, minRa))
  }

  /**
   * Tuple de coordonnées d'un rectangle à partir d'une ligne de l'index
   */
  def indexLigneToCoord(ligne: Array[String]): ((Double, Double), (Double, Double)) = {
    coordTuple(ligne(minDeclPos).toDouble, ligne(maxDeclPos).toDouble, ligne(minRaPos).toDouble, ligne(maxRaPos).toDouble)
  }

  /**
   * Tuple de coordonnées d'un point à partir d'une ligne de partition
   */
  def partitionLigneToCoord(ligne: Array[String]): (Double, Double) = {
    (ligne(declPos).toDouble, ligne(raPos).toDouble)
  }


  /**
   * Retourne vrai si le point se trouve dans le rectangle donné en paramètre
   */
  def isPointInRect(point: (Double, Double), rect: ((Double, Double), (Double, Double))): Boolean = {
    (point._1 >= rect._1._1 && point._1 <= rect._2._1) && (point._2 <= rect._1._2 && point._2 >= rect._2._2)
  }

  /**
   * Retourne vrai si une partie de rect2 est comprise dans rect1
   */
  def doOverlap(rect1: ((Double, Double), (Double, Double)), rect2: ((Double, Double), (Double, Double))): Boolean = {
    !((rect1._1._1 >= rect2._2._1 || rect2._1._1 >= rect1._2._1) || (rect1._1._2 <= rect2._2._2 || rect2._1._2 <= rect1._2._2))
  }

  def loadPartition(path: String, sc: SparkContext): RDD[Array[String]] = {
    sc.textFile(path).filter(el => !el.isEmpty).map(_.split(",").map(_.trim))
  }

  def main(args: Array[String]): Unit = {
    if (args.length == 6) {
      //val indexPath = "index/"
      val indexPath = args(0)
      //val outputPath = "res/"
      val outputPath = args(1)
      val conf = new SparkConf().setAppName("requete-" + compte)
      val sc = new SparkContext(conf)

      val searchRectangle = coordTuple(args(2).toDouble, args(3).toDouble, args(4).toDouble, args(5).toDouble)

      // On charge l'index
      val index = sc.textFile(indexPath + "/indexFile")
        .filter(el => !el.isEmpty)
        .map(_.split(",").map(_.trim))

      // On détermine les partition dans lesquelles rechercher
      val partitionsToLoad = index
        .filter(el => doOverlap(indexLigneToCoord(el), searchRectangle)) // Check si la partition est dans le rectangle d'observation
        .map(el => el(0))
        .collect()

      for (i <- partitionsToLoad.indices) {
        loadPartition(indexPath + "/partition_" + partitionsToLoad(i), sc) // Chargement de la partiton à explorer
          .filter(el => isPointInRect(partitionLigneToCoord(el), searchRectangle)) // Check si la ligne est dans le rectangle d'observation
          .map(el => el.mkString(",") + "\n")
          .saveAsTextFile(outputPath + "/resultatRequete_" + i)
      }


      println("Résultat sauvegardé dans " + outputPath + "/resultatRequete")
    } else {

      println("Usage: spark-submit --class SparkTPApp3 /home/\" + compte + \"/SparkTPApp-assembly-1.0.jar path/to/index output/path minDecl maxDecl minRa maxRa")
    }
  }
}
