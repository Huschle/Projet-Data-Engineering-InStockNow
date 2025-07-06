import cats.effect.{IO, IOApp, ExitCode}
import fs2.kafka._
import io.circe.generic.auto._
import io.circe.parser._
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

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

  val seasonalPriority: Map[String, Set[String]] = Map(
    "summer" -> Set("maillot de bain", "crème solaire"),
    "winter" -> Set("ski", "doudoune"),
    "spring" -> Set("t-shirt"),
    "autumn" -> Set("parapluie", "pull")
  )

  def getSeasonFromTimestamp(ts: String): Option[String] = {
    try {
      val date = ZonedDateTime.parse(ts, DateTimeFormatter.ISO_ZONED_DATE_TIME)
      val month = date.getMonthValue
      Some(month match {
        case m if Set(12, 1, 2).contains(m) => "winter"
        case m if Set(3, 4, 5).contains(m) => "spring"
        case m if Set(6, 7, 8).contains(m) => "summer"
        case _ => "autumn"
      })
    } catch {
      case _: Exception => None
    }
  }

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
          case Right(msg) =>
            val missingQty = 3 * msg.threshold - msg.quantity

            getSeasonFromTimestamp(msg.timestamp) match {
              case Some(season) =>
                val priorityProducts = seasonalPriority.getOrElse(season, Set.empty)
                val isHighPriority = priorityProducts.contains(msg.product_id)

                IO(println(s"Missing product: '${msg.product_id}', missing quantity: $missingQty, high priority: $isHighPriority"))

              case None =>
                IO(println(s"Invalid timestamp format in message: ${msg.timestamp}"))
            }

          case Left(error) =>
            IO(println(s"Failed to parse message: $error\nRaw: $value"))
        }
      }
      .compile
      .drain
      .as(ExitCode.Success)
  }
}
