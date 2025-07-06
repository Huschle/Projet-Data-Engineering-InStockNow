import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Main extends App {

  // Configuration Spark optimisée avec logs supprimés
  val spark = SparkSession.builder()
    .appName("StockAnalysis")
    .master("local[2]")
    .config("spark.hadoop.fs.defaultFS", sys.env.getOrElse("HDFS_NAMENODE_URL", "hdfs://localhost:9000"))
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.ui.showConsoleProgress", "false")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64MB")
    .getOrCreate()

  // Supprimer tous les logs Spark
  spark.sparkContext.setLogLevel("OFF")
  
  // Supprimer les logs Hadoop
  val hadoopLogger = org.apache.log4j.Logger.getLogger("org.apache.hadoop")
  hadoopLogger.setLevel(org.apache.log4j.Level.OFF)
  
  // Supprimer les logs Jetty
  val jettyLogger = org.apache.log4j.Logger.getLogger("org.sparkproject.jetty")
  jettyLogger.setLevel(org.apache.log4j.Level.OFF)

  import spark.implicits._

  val schema = StructType(Array(
    StructField("sensor_id", StringType, true),
    StructField("timestamp", StringType, true),
    StructField("product_id", StringType, true),
    StructField("quantity", IntegerType, true),
    StructField("threshold", IntegerType, true),
    StructField("store", StringType, true),
    StructField("lat", DoubleType, true),
    StructField("lon", DoubleType, true),
    StructField("alert", BooleanType, true)
  ))

  def analyzeData(): Unit = {
    try {
      println("=== CHARGEMENT DES DONNÉES ===")
      
      // Lecture des données avec gestion d'erreurs
      val df = spark.read
        .option("delimiter", "|")
        .option("multiline", "false")
        .option("ignoreLeadingWhiteSpace", "true")
        .option("ignoreTrailingWhiteSpace", "true")
        .schema(schema)
        .csv(s"${sys.env.getOrElse("HDFS_NAMENODE_URL", "hdfs://localhost:9000")}/stock-data/*/*/*/*/*.txt")
        .filter($"sensor_id".isNotNull && $"product_id".isNotNull)
        .withColumn("timestamp", to_timestamp($"timestamp", "yyyy-MM-dd HH:mm:ss"))
        .withColumn("hour", hour($"timestamp"))
        .withColumn("day_of_week", dayofweek($"timestamp"))
        .withColumn("is_weekend", $"day_of_week".isin(1, 7))
        .coalesce(4)
        .cache()

      val totalCount = df.count()
      println(s"Nombre total de messages: $totalCount")
      
      if (totalCount == 0) {
        println("Aucune donnée trouvée. Vérifiez que le composant 4 a bien stocké des données.")
        return
      }
      
      println()
      println("=== ANALYSE DES DONNÉES DE STOCK ===")

      // Question 1: Y a-t-il plus d'alertes pendant le week-end ou en semaine ?
      println("1. Répartition des alertes week-end vs semaine:")
      val alertsWeekend = df.filter($"alert" === true && $"is_weekend" === true).count()
      val alertsWeekday = df.filter($"alert" === true && $"is_weekend" === false).count()
      println(s"Week-end: $alertsWeekend alertes")
      println(s"Semaine: $alertsWeekday alertes")
      println()

      // Question 2: Quel produit génère le plus d'alertes ?
      println("2. Top 5 des produits générant le plus d'alertes:")
      val topProducts = df.filter($"alert" === true)
        .groupBy($"product_id")
        .count()
        .orderBy(desc("count"))
        .limit(5)
        .collect()
      
      topProducts.foreach(row => 
        println(s"${row.getString(0)}: ${row.getLong(1)} alertes")
      )
      println()

      // Question 3: À quelle heure y a-t-il le plus d'alertes ?
      println("3. Répartition des alertes par heure (Top 5):")
      val topHours = df.filter($"alert" === true)
        .groupBy($"hour")
        .count()
        .orderBy(desc("count"))
        .limit(5)
        .collect()
      
      topHours.foreach(row => 
        println(s"${row.getInt(0)}h: ${row.getLong(1)} alertes")
      )
      println()

      // Question 4: Quel est le niveau de stock moyen par produit ?
      println("4. Niveau de stock moyen par produit:")
      val stockLevels = df.groupBy($"product_id")
        .agg(
          avg($"quantity").alias("stock_moyen"),
          avg($"threshold").alias("seuil_moyen"),
          count("*").alias("nombre_mesures")
        )
        .orderBy($"product_id")
        .collect()
      
      stockLevels.foreach(row => 
        println(f"${row.getString(0)}: Stock moyen = ${row.getDouble(1)}%.2f, Seuil = ${row.getDouble(2)}%.2f, Mesures = ${row.getLong(3)}")
      )
      println()

      // Question 5: Évolution du stock dans le temps
      println("5. Évolution du stock moyen par heure:")
      val hourlyStock = df.groupBy($"hour")
        .agg(
          avg($"quantity").alias("stock_moyen_heure"),
          countDistinct($"product_id").alias("produits_distincts")
        )
        .orderBy($"hour")
        .collect()
      
      hourlyStock.foreach(row => 
        println(f"${row.getInt(0)}h: Stock moyen = ${row.getDouble(1)}%.2f, Produits distincts = ${row.getLong(2)}")
      )

    } catch {
      case ex: Exception =>
        println(s"Erreur lors de l'analyse: ${ex.getMessage}")
        println("Vérifiez que:")
        println("1. HDFS est démarré (docker-compose up -d)")
        println("2. Le composant 4 a stocké des données")
        println("3. Le chemin HDFS est correct")
        ex.printStackTrace()
    }
  }

  analyzeData()
  spark.stop()
  println("\n=== ANALYSE TERMINÉE ===")
}