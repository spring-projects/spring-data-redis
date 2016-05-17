package org.springframework.data.redis.core;

/**
 * Created by ndivadkar on 3/29/16.
 */
public class GeoCoordinate {
    private double longitude;
    private double latitude;

    public GeoCoordinate(double longitude, double latitude) {
        this.longitude = longitude;
        this.latitude = latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public double getLatitude() {
        return latitude;
    }
}
