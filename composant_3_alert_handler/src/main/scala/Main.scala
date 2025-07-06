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
        .withBootstrapServers("localhost:9092")
        .withGroupId("instocknow-alert-handler")
        .withAutoOffsetReset(AutoOffsetReset.Earliest)

    KafkaConsumer
      .stream(consumerSettings)
      .subscribeTo("instocknow-alerts")
      .records
      .evalMap { committable =>
        val value = committable.record.value
        decode[StockMessage](value) match {
          case Right(msg) => IO(println(s"ALERT: $msg"))
          case Left(error) => IO(println(s"Failed to parse message: $error\nRaw: $value"))
        }
      }
      .compile
      .drain
      .as(ExitCode.Success)
  }
}
