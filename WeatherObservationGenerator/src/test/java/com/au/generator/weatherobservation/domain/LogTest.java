package com.au.generator.weatherobservation.domain;

import org.junit.Test;

import java.time.Instant;

import static org.junit.Assert.assertEquals;

public class LogTest {

    @Test
    public void toString_should_return_log_well_formatted() {
        // GIVEN
        final String expectedResult = "2017-10-10T09:10|48.51;2.21|22|FR\n";
        final Instant instant = Instant.parse("2017-10-10T10:10:10Z");
        final Log log = new Log(instant, 48.51, 2.21, 22, "FR"); // Paris France

        // WHEN
        final String result = log.toString();

        // THEN
        assertEquals(expectedResult, result);
    }
}