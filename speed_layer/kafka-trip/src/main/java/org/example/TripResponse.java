
package org.example;

import java.util.LinkedHashMap;
import java.util.Map;
import javax.annotation.Generated;
import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
    "hvfhs_license_num",
    "dispatching_base_num",
    "originating_base_num",
    "request_datetime",
    "on_scene_datetime",
    "pickup_datetime",
    "dropoff_datetime",
    "pulocationid",
    "dolocationid",
    "trip_miles",
    "trip_time",
    "base_passenger_fare",
    "tolls",
    "bcf",
    "sales_tax",
    "congestion_surcharge",
    "airport_fee",
    "tips",
    "driver_pay",
    "shared_request_flag",
    "shared_match_flag",
    "wav_request_flag",
    "wav_match_flag"
})
@Generated("jsonschema2pojo")
public class TripResponse {

    @JsonProperty("hvfhs_license_num")
    private String hvfhsLicenseNum;
    @JsonProperty("dispatching_base_num")
    private String dispatchingBaseNum;
    @JsonProperty("originating_base_num")
    private String originatingBaseNum;
    @JsonProperty("request_datetime")
    private String requestDatetime;
    @JsonProperty("on_scene_datetime")
    private String onSceneDatetime;
    @JsonProperty("pickup_datetime")
    private String pickupDatetime;
    @JsonProperty("dropoff_datetime")
    private String dropoffDatetime;
    @JsonProperty("pulocationid")
    private String pulocationid;
    @JsonProperty("dolocationid")
    private String dolocationid;
    @JsonProperty("trip_miles")
    private String tripMiles;
    @JsonProperty("trip_time")
    private String tripTime;
    @JsonProperty("base_passenger_fare")
    private String basePassengerFare;
    @JsonProperty("tolls")
    private String tolls;
    @JsonProperty("bcf")
    private String bcf;
    @JsonProperty("sales_tax")
    private String salesTax;
    @JsonProperty("congestion_surcharge")
    private String congestionSurcharge;
    @JsonProperty("airport_fee")
    private String airportFee;
    @JsonProperty("tips")
    private String tips;
    @JsonProperty("driver_pay")
    private String driverPay;
    @JsonProperty("shared_request_flag")
    private String sharedRequestFlag;
    @JsonProperty("shared_match_flag")
    private String sharedMatchFlag;
    @JsonProperty("wav_request_flag")
    private String wavRequestFlag;
    @JsonProperty("wav_match_flag")
    private String wavMatchFlag;
    @JsonIgnore
    private Map<String, Object> additionalProperties = new LinkedHashMap<String, Object>();

    @JsonProperty("hvfhs_license_num")
    public String getHvfhsLicenseNum() {
        return hvfhsLicenseNum;
    }

    @JsonProperty("hvfhs_license_num")
    public void setHvfhsLicenseNum(String hvfhsLicenseNum) {
        this.hvfhsLicenseNum = hvfhsLicenseNum;
    }

    @JsonProperty("dispatching_base_num")
    public String getDispatchingBaseNum() {
        return dispatchingBaseNum;
    }

    @JsonProperty("dispatching_base_num")
    public void setDispatchingBaseNum(String dispatchingBaseNum) {
        this.dispatchingBaseNum = dispatchingBaseNum;
    }

    @JsonProperty("originating_base_num")
    public String getOriginatingBaseNum() {
        return originatingBaseNum;
    }

    @JsonProperty("originating_base_num")
    public void setOriginatingBaseNum(String originatingBaseNum) {
        this.originatingBaseNum = originatingBaseNum;
    }

    @JsonProperty("request_datetime")
    public String getRequestDatetime() {
        return requestDatetime;
    }

    @JsonProperty("request_datetime")
    public void setRequestDatetime(String requestDatetime) {
        this.requestDatetime = requestDatetime;
    }

    @JsonProperty("on_scene_datetime")
    public String getOnSceneDatetime() {
        return onSceneDatetime;
    }

    @JsonProperty("on_scene_datetime")
    public void setOnSceneDatetime(String onSceneDatetime) {
        this.onSceneDatetime = onSceneDatetime;
    }

    @JsonProperty("pickup_datetime")
    public String getPickupDatetime() {
        return pickupDatetime;
    }

    @JsonProperty("pickup_datetime")
    public void setPickupDatetime(String pickupDatetime) {
        this.pickupDatetime = pickupDatetime;
    }

    @JsonProperty("dropoff_datetime")
    public String getDropoffDatetime() {
        return dropoffDatetime;
    }

    @JsonProperty("dropoff_datetime")
    public void setDropoffDatetime(String dropoffDatetime) {
        this.dropoffDatetime = dropoffDatetime;
    }

    @JsonProperty("pulocationid")
    public String getPulocationid() {
        return pulocationid;
    }

    @JsonProperty("pulocationid")
    public void setPulocationid(String pulocationid) {
        this.pulocationid = pulocationid;
    }

    @JsonProperty("dolocationid")
    public String getDolocationid() {
        return dolocationid;
    }

    @JsonProperty("dolocationid")
    public void setDolocationid(String dolocationid) {
        this.dolocationid = dolocationid;
    }

    @JsonProperty("trip_miles")
    public String getTripMiles() {
        return tripMiles;
    }

    @JsonProperty("trip_miles")
    public void setTripMiles(String tripMiles) {
        this.tripMiles = tripMiles;
    }

    @JsonProperty("trip_time")
    public String getTripTime() {
        return tripTime;
    }

    @JsonProperty("trip_time")
    public void setTripTime(String tripTime) {
        this.tripTime = tripTime;
    }

    @JsonProperty("base_passenger_fare")
    public String getBasePassengerFare() {
        return basePassengerFare;
    }

    @JsonProperty("base_passenger_fare")
    public void setBasePassengerFare(String basePassengerFare) {
        this.basePassengerFare = basePassengerFare;
    }

    @JsonProperty("tolls")
    public String getTolls() {
        return tolls;
    }

    @JsonProperty("tolls")
    public void setTolls(String tolls) {
        this.tolls = tolls;
    }

    @JsonProperty("bcf")
    public String getBcf() {
        return bcf;
    }

    @JsonProperty("bcf")
    public void setBcf(String bcf) {
        this.bcf = bcf;
    }

    @JsonProperty("sales_tax")
    public String getSalesTax() {
        return salesTax;
    }

    @JsonProperty("sales_tax")
    public void setSalesTax(String salesTax) {
        this.salesTax = salesTax;
    }

    @JsonProperty("congestion_surcharge")
    public String getCongestionSurcharge() {
        return congestionSurcharge;
    }

    @JsonProperty("congestion_surcharge")
    public void setCongestionSurcharge(String congestionSurcharge) {
        this.congestionSurcharge = congestionSurcharge;
    }

    @JsonProperty("airport_fee")
    public String getAirportFee() {
        return airportFee;
    }

    @JsonProperty("airport_fee")
    public void setAirportFee(String airportFee) {
        this.airportFee = airportFee;
    }

    @JsonProperty("tips")
    public String getTips() {
        return tips;
    }

    @JsonProperty("tips")
    public void setTips(String tips) {
        this.tips = tips;
    }

    @JsonProperty("driver_pay")
    public String getDriverPay() {
        return driverPay;
    }

    @JsonProperty("driver_pay")
    public void setDriverPay(String driverPay) {
        this.driverPay = driverPay;
    }

    @JsonProperty("shared_request_flag")
    public String getSharedRequestFlag() {
        return sharedRequestFlag;
    }

    @JsonProperty("shared_request_flag")
    public void setSharedRequestFlag(String sharedRequestFlag) {
        this.sharedRequestFlag = sharedRequestFlag;
    }

    @JsonProperty("shared_match_flag")
    public String getSharedMatchFlag() {
        return sharedMatchFlag;
    }

    @JsonProperty("shared_match_flag")
    public void setSharedMatchFlag(String sharedMatchFlag) {
        this.sharedMatchFlag = sharedMatchFlag;
    }

    @JsonProperty("wav_request_flag")
    public String getWavRequestFlag() {
        return wavRequestFlag;
    }

    @JsonProperty("wav_request_flag")
    public void setWavRequestFlag(String wavRequestFlag) {
        this.wavRequestFlag = wavRequestFlag;
    }

    @JsonProperty("wav_match_flag")
    public String getWavMatchFlag() {
        return wavMatchFlag;
    }

    @JsonProperty("wav_match_flag")
    public void setWavMatchFlag(String wavMatchFlag) {
        this.wavMatchFlag = wavMatchFlag;
    }

    @JsonAnyGetter
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    @JsonAnySetter
    public void setAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
    }

}
