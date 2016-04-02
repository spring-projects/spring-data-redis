package org.springframework.data.redis.core;

/**
 * Created by ndivadkar on 4/1/16.
 */
public class GeoRadiusResponse {
  private byte[] member;
  private double distance;
  private GeoCoordinate coordinate;

  public GeoRadiusResponse(byte[] member) {
    this.member = member;
  }

  public void setDistance(double distance) {
    this.distance = distance;
  }

  public void setCoordinate(GeoCoordinate coordinate) {
    this.coordinate = coordinate;
  }

  public byte[] getMember() {
    return member;
  }

  public double getDistance() {
    return distance;
  }

  public GeoCoordinate getCoordinate() {
    return coordinate;
  }
}
