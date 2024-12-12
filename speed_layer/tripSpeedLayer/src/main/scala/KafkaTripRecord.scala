import scala.reflect.runtime.universe._
import java.sql.Timestamp

// All fields are wrapped in Option to handle potential null values
import com.fasterxml.jackson.annotation.JsonProperty
import java.sql.Timestamp

case class KafkaTripRecord(
                            @JsonProperty("hvfhs_license_num") hvfhsLicenseNum: Option[String],
                            @JsonProperty("dispatching_base_num") dispatchingBaseNum: Option[String],
                            @JsonProperty("originating_base_num") originatingBaseNum: Option[String],
                            @JsonProperty("request_datetime") requestDatetime: Option[String],
                            @JsonProperty("on_scene_datetime") onSceneDatetime: Option[String],
                            @JsonProperty("pickup_datetime") pickupDatetime: Option[String],
                            @JsonProperty("dropoff_datetime") dropoffDatetime: Option[String],
                            @JsonProperty("pulocationid") puLocationId: Option[String],
                            @JsonProperty("dolocationid") doLocationId: Option[String],
                            @JsonProperty("trip_miles") tripMiles: Option[String],
                            @JsonProperty("trip_time") tripTime: Option[String],
                            @JsonProperty("base_passenger_fare") basePassengerFare: Option[String],
                            @JsonProperty("tolls") tolls: Option[String],
                            @JsonProperty("bcf") bcf: Option[String],
                            @JsonProperty("sales_tax") salesTax: Option[String],
                            @JsonProperty("congestion_surcharge") congestionSurcharge: Option[String],
                            @JsonProperty("airport_fee") airportFee: Option[String],
                            @JsonProperty("tips") tips: Option[String],
                            @JsonProperty("driver_pay") driverPay: Option[String],
                            @JsonProperty("shared_request_flag") sharedRequestFlag: Option[String],
                            @JsonProperty("shared_match_flag") sharedMatchFlag: Option[String],
                            @JsonProperty("access_a_ride_flag") accessARideFlag: Option[String],
                            @JsonProperty("wav_request_flag") wavRequestFlag: Option[String],
                            @JsonProperty("wav_match_flag") wavMatchFlag: Option[String]
                          )



