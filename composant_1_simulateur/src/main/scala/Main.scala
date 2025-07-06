import cats.effect.{ExitCode, IO, IOApp}
import fs2.kafka._
import io.circe.syntax._
import io.circe.generic.auto._
import scala.util.Random
import java.time.Instant
import scala.concurrent.duration._
import cats.implicits._

object Main extends IOApp {

    val productThresholds: Map[String, Int] = Map(
    "ski" -> 3,
    "doudoune" -> 4,
    "pull" -> 4,
    "maillot de bain" -> 4,
    "t-shirt" -> 4,
    "crème solaire" -> 4,
    "parapluie" -> 3,
    "GTA6" -> 2,
    "coca" -> 4,
    "papier toilette" -> 4
    )

    val allProducts: List[String] = productThresholds.keys.toList


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

    def generateMessage(product: String, threshold: Int): StockMessage = {
    val quantity = Random.between(0, 30)

    StockMessage(
        sensor_id = s"shelf-${Random.between(1, 5)}",
        timestamp = Instant.now().toString,
        product_id = product,
        quantity = quantity,
        threshold = threshold,
        location = Location("paris-12", 48.85, 2.34),
        alert = quantity < threshold
    )
    }



  override def run(args: List[String]): IO[ExitCode] = {

    val producerSettings =
      ProducerSettings[IO, String, String]
        .withBootstrapServers(sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))

    KafkaProducer.stream(producerSettings).flatMap { producer =>

        fs2.Stream.awakeEvery[IO](2.seconds).evalMap { _ =>
            val messages = allProducts.map { product =>
                val threshold = productThresholds.getOrElse(product, 5)
                generateMessage(product, threshold)
        }

        messages.parTraverse { msg =>
            val json = msg.asJson.noSpaces
            val record = ProducerRecord("instocknow-input", msg.sensor_id, json)
            println(s"Envoi → ${msg.product_id} | quantity = ${msg.quantity} | threshold = ${msg.threshold}")
            producer.produce(ProducerRecords.one(record)).void
        }.void
        }
    }.compile.drain.as(ExitCode.Success)
  }
}
