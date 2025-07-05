import cats.effect.{ExitCode, IO, IOApp}
import fs2.kafka._
import io.circe.parser._
import io.circe.generic.auto._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import java.io.{BufferedWriter, OutputStreamWriter}
import java.time.{Instant, LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter
import cats.implicits._

object Main extends IOApp {

  final case class Location(store: String, lat: Double, lon: Double)
  final case class StockMessage(
    sensor_id: String,
    timestamp: String,
    product_id: String,
    quantity: Int,
    threshold: Int,
    location: Location,
    alert: Boolean
  )

  val hadoopConf = new Configuration()
  hadoopConf.set("fs.defaultFS", "hdfs://localhost:9000")
  
  def writeToHDFS(data: String, path: String): IO[Unit] = IO {
    val fs = FileSystem.get(hadoopConf)
    val hdfsPath = new Path(path)
    
    // Créer le répertoire parent s'il n'existe pas
    val parentPath = hdfsPath.getParent
    if (!fs.exists(parentPath)) {
      fs.mkdirs(parentPath)
    }
    
    // Écrire le fichier
    val outputStream = fs.create(hdfsPath, true)
    val writer = new BufferedWriter(new OutputStreamWriter(outputStream))
    writer.write(data)
    writer.newLine()
    writer.close()
    outputStream.close()
    fs.close()
  }

  def formatMessageForStorage(message: StockMessage): String = {
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    val timestamp = Instant.parse(message.timestamp)
    val formattedTimestamp = LocalDateTime.ofInstant(timestamp, ZoneOffset.UTC).format(formatter)
    
    s"${message.sensor_id}|${formattedTimestamp}|${message.product_id}|${message.quantity}|${message.threshold}|${message.location.store}|${message.location.lat}|${message.location.lon}|${message.alert}"
  }

  def generateHDFSPath(message: StockMessage): String = {
    val timestamp = Instant.parse(message.timestamp)
    val date = LocalDateTime.ofInstant(timestamp, ZoneOffset.UTC)
    val year = date.getYear
    val month = f"${date.getMonthValue}%02d"
    val day = f"${date.getDayOfMonth}%02d"
    val hour = f"${date.getHour}%02d"
    
    s"/stock-data/year=$year/month=$month/day=$day/hour=$hour/${message.sensor_id}_${System.currentTimeMillis()}.txt"
  }

  override def run(args: List[String]): IO[ExitCode] = {
    val consumerSettings =
      ConsumerSettings[IO, String, String]
        .withBootstrapServers("localhost:9092")
        .withGroupId("storage-consumer")
        .withAutoOffsetReset(AutoOffsetReset.Latest)

    KafkaConsumer.stream(consumerSettings)
      .subscribeTo("instocknow-input")
      .records
      .evalMap { record =>
        val jsonString = record.record.value
        decode[StockMessage](jsonString) match {
          case Right(message) =>
            val formattedData = formatMessageForStorage(message)
            val hdfsPath = generateHDFSPath(message)
            writeToHDFS(formattedData, hdfsPath) *>
              IO.println(s"Stocké → ${message.product_id} dans $hdfsPath")
          case Left(error) =>
            IO.println(s"Erreur parsing JSON: $error")
        }
      }
      .compile
      .drain
      .as(ExitCode.Success)
  }
}