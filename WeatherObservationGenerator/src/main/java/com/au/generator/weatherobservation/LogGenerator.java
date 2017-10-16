package com.au.generator.weatherobservation;

import com.au.generator.weatherobservation.domain.Log;
import com.google.common.collect.Lists;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Random;
import java.util.logging.Logger;

import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;

public class LogGenerator {
    private static final Logger LOGGER = Logger.getLogger(LogGenerator.class.getName());

    private static final double MIN_LATITUDE = 0.0;
    private static final double MAX_LATITUDE = 90.0;
    private static final double MIN_LONGITUDE = -180.0;
    private static final double MAX_LONGITUDE = 180.0;
    private static final double MIN_TEMP = -22.0;
    private static final double MAX_TEMP = 50.0;

    private static final List<String> observatories = Lists.newArrayList("AU", "FR", "BE", "US", "CA", "NZ");

    public void generateWeatherObservations(final int rowNumber, final Path logFile) throws IOException {
        byte[] data;
        try (OutputStream out = new BufferedOutputStream(Files.newOutputStream(logFile, CREATE, APPEND))) {

            for (int i = 1; i <= rowNumber; i++) {
                final Log log = generateLog(Instant.now());
                data = log.toString().getBytes(Charset.defaultCharset());
                out.write(data);
            }

        } catch (IOException e) {
            LOGGER.severe(e.getMessage());
            throw new IOException(e);
        }
    }

    private Log generateLog(final Instant instant) {
        final double latitude = randomCoordinateLatitude();
        double longitude = randomCoordinateLongitude();
        String observatory = getRandomObservatory();

        double temp = observatory.equals("US") ? randomFarheneit() : randomDegree();

        return new Log(instant, longitude, latitude, temp, observatory);
    }

    private double randomDegree() {
        final Random random = new Random();
        return MIN_TEMP + (MAX_TEMP - MIN_TEMP) * random.nextDouble();
    }

    private double randomFarheneit() {
        double degreCelcius = randomDegree();
        return 1.8 * degreCelcius + 32;
    }

    private String getRandomObservatory() {
        Random random = new Random();
        int rnd = random.nextInt(observatories.size());

        return observatories.get(rnd);
    }

    private double randomCoordinateLatitude() {
        return randomCoordinate(MIN_LATITUDE, MAX_LATITUDE);
    }

    private double randomCoordinateLongitude() {
        return randomCoordinate(MIN_LONGITUDE, MAX_LONGITUDE);
    }

    private double randomCoordinate(final double min, final double max) {
        final Random random = new Random();
        return min + (max - min) * random.nextDouble();
    }
}
