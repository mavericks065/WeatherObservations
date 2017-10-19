package com.au.generator.weatherobservation;


import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.logging.Logger;


public class Main {

    private static final Logger LOGGER = Logger.getLogger(Main.class.getName());
    private static final String LOG_FILE = "./weather-observations.log";
    private static final int ROW_NB = 1000;

    public static void main(String[] args) throws IOException {
        final LogGenerator generator = new LogGenerator();

        final long startDate = System.currentTimeMillis();
        final Path logFile = Paths.get(LOG_FILE);

        generator.generateWeatherObservations(ROW_NB, logFile);

        final long endDate = System.currentTimeMillis();
        double timeTaken = (endDate - startDate) / (1000.0);

        LOGGER.info("done!");
        LOGGER.info("It took : " + Double.toString(timeTaken));
    }
}
