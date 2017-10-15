package com.au.generator.weatherobservation.domain;

import java.text.DecimalFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class Log {

    private final Instant timestamp;
    private final double longitude;
    private final double latitude;
    private final double temperature;
    private final String observatory;

    public Log(final Instant timestamp, final double longitude, final double latitude, final double temperature,
               final String observatory) {
        this.timestamp = timestamp;
        this.longitude = longitude;
        this.latitude = latitude;
        this.temperature = temperature;
        this.observatory = observatory;
    }

    @Override
    public String toString() {
        final DecimalFormat decimalFormatter = new DecimalFormat("#.##");

        final StringBuffer sb = new StringBuffer(formatTimeStamp());

        sb.append("|").append(decimalFormatter.format(longitude)).append(",").append(decimalFormatter.format(latitude));
        sb.append("|").append(decimalFormatter.format(temperature));
        sb.append("|").append(observatory);
        sb.append("\n");

        return sb.toString();
    }

    private String formatTimeStamp() {
        final DateTimeFormatter formatter =
                DateTimeFormatter.ofPattern("yyyy-MM-dd'T'hh:mm")
                    .withZone(ZoneId.systemDefault());
        return formatter.format(timestamp);
    }
}
