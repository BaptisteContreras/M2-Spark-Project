import PartitionN.partionne_en_N_Rec
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Index {
  val compte = "baptFlo"

  val declPos = 9
  val raPos = 6


  def file_to_RDD(src: String, attrPosition: Int, sc: SparkContext): RDD[(Double, Array[String])] = {
    sc.textFile(src)
      .map(_.split(",").map(_.trim))
      .map(el => (el(attrPosition).toDouble, el))
  }

  def searchMinOf(attributIndex: Int, rdd: RDD[Array[String]]): String = {
    rdd
      .map(el => el(attributIndex))
      .reduce((a, b) => {
        if (a.toDouble > b.toDouble) {
          b
        } else {
          a
        }
      })
  }

  def searchMaxOf(attributIndex: Int, rdd: RDD[Array[String]]): String = {
    rdd
      .map(el => el(attributIndex))
      .reduce((a, b) => {
        if (a.toDouble > b.toDouble) {
          a
        } else {
          b
        }
      })
  }

  def main(args: Array[String]): Unit = {
    if (args.length == 3) {
      //   var srcFile = "/tp-data/Source/Source-001.csv"
      //      srcFile = "source-sample"
      val nbPartition = args(1).toInt
      // val destIndex = "index/"
      val destIndex = args(2)
      val conf = new SparkConf().setAppName("createIndex-" + compte)
      val sc = new SparkContext(conf)
      val baseSet = file_to_RDD(args(0), declPos, sc)
      val res = partionne_en_N_Rec(baseSet, nbPartition, sc)
      var i = 0

      // Création des partitions

      res.foreach(partition => {
        partionne_en_N_Rec(partition.map(el => (el._2(raPos).toDouble, el._2)), nbPartition, sc)
          .foreach(subPartition => {
            subPartition.map(el => el._2.mkString(",") + "\n").saveAsTextFile(destIndex + "/partition_" + i)
            i = i + 1
          })
      })

      // Tableau qui contient l'index qu'on va ensuite écrire dans un fichier
      // [ (numéroPartition, (minDecl, maxDecl, minRa, maxRa)) ]
      var index: Array[(Int, (String, String, String, String))] = Array()
      // On crée l'index en calculant le min / max pour chaque partition
      for (partitionIndex <- 0 until i) {
        val partitionLoaded = sc.textFile(destIndex + "/partition_" + partitionIndex)
          .filter(el => !el.isEmpty)
          .map(_.split(",").map(_.trim)).cache() // On le cache car on va l'utiliser 4 fois

        // [ (numéroPartition, (minDecl, maxDecl, minRa, maxRa)) ]
        index :+= ((partitionIndex, (searchMinOf(declPos, partitionLoaded), searchMaxOf(declPos, partitionLoaded), searchMinOf(raPos, partitionLoaded), searchMaxOf(raPos, partitionLoaded))))
      }

      // numéroPartition,minDecl, maxDecl, minRa, maxRa
      sc.parallelize(index).map(el => el._1 + "," + el._2._1 + "," + el._2._2 + "," + el._2._3 + "," + el._2._4).saveAsTextFile(destIndex + "/indexFile")

    } else {
      println("Usage: spark-submit --class SparkTPApp3 /home/\" + compte + \"/SparkTPApp-assembly-1.0.jar path/to/sources nbPartition location/new/index")
    }


  }
}
