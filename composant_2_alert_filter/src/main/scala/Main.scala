import cats.effect.{IO, IOApp, ExitCode}
import fs2.kafka._
import io.circe.generic.auto._
import io.circe.parser._

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

  override def run(args: List[String]): IO[ExitCode] = {
    val consumerSettings =
      ConsumerSettings[IO, String, String]
        .withBootstrapServers(sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
        .withGroupId("instocknow-alert-filter")
        .withAutoOffsetReset(AutoOffsetReset.Earliest)

    val producerSettings =
      ProducerSettings[IO, String, String]
        .withBootstrapServers(sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))

    KafkaProducer.resource(producerSettings).use { producer => 
      KafkaConsumer
        .stream(consumerSettings)
        .subscribeTo("instocknow-input")
        .records
        .evalMap { committable =>
          val value = committable.record.value
          decode[StockMessage](value) match {
            case Right(msg) if msg.alert =>
              val record = ProducerRecord("instocknow-alerts", msg.sensor_id, value)
              producer.produce(ProducerRecords.one(record)).void
            case _ => IO.unit
          }
        }
        .compile
        .drain
        .as(ExitCode.Success)
    }
  }
}
