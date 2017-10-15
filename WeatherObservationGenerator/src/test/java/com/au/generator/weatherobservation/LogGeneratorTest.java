package com.au.generator.weatherobservation;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class LogGeneratorTest {

    private static final String OUTPUTFILE = "./test.log";

    @After
    public void tearDown() {
        File file = new File(OUTPUTFILE);
        file.delete();
    }

    @Test
    public void generateWeatherObservations_should_generate_log_files() throws IOException {
        // GIVEN
        final Path path = Paths.get(OUTPUTFILE);
        final int rows = 10;
        final LogGenerator generator = new LogGenerator();

        // WHEN
        generator.generateWeatherObservations(rows, path);

        // THEN
        Assert.assertTrue(Files.exists(path));
    }

    @Test
    public void generateWeatherObservations_should_generate_log_files_with_the_number_of_lines_passed() throws IOException {
        // GIVEN
        final Path path = Paths.get(OUTPUTFILE);
        final int rows = 10;
        final LogGenerator generator = new LogGenerator();

        // WHEN
        generator.generateWeatherObservations(rows, path);
        final int actualResult = getNumberOfRows(OUTPUTFILE);

        // THEN
        Assert.assertEquals(rows, actualResult);
    }

    private int getNumberOfRows(String fileName) throws IOException {
        final List<String> lines = Files.readAllLines(Paths.get(fileName), Charset.defaultCharset());
        return lines.size();
    }
}