package pool;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ThreadPoolExperimentResultTest {

    @Test
    void shouldRecognizeCompleteExperiment() {
        ThreadPoolExperimentResult result =
                new ThreadPoolExperimentResult(
                        "BWS",
                        1000,
                        1000,
                        0,
                        0,
                        0,
                        20,
                        500,
                        700,
                        true,
                        true
                );

        assertEquals(1000, result.getResolvedTaskCount());
        assertEquals(1.0, result.getCompletionRate(), 0.0001);
        assertTrue(result.isAccountingComplete());
        assertTrue(result.isRunComplete());
    }

    @Test
    void shouldRecognizeUnresolvedTasks() {
        ThreadPoolExperimentResult result =
                new ThreadPoolExperimentResult(
                        "BISS",
                        1000,
                        980,
                        0,
                        10,
                        10,
                        15,
                        500,
                        700,
                        false,
                        true
                );

        assertEquals(990, result.getResolvedTaskCount());
        assertFalse(result.isAccountingComplete());
        assertFalse(result.isRunComplete());
    }

    @Test
    void shouldDistinguishAccountingFromTermination() {
        ThreadPoolExperimentResult result =
                new ThreadPoolExperimentResult(
                        "JDK",
                        1000,
                        970,
                        0,
                        30,
                        0,
                        0,
                        480,
                        680,
                        true,
                        false
                );

        assertTrue(result.isAccountingComplete());
        assertFalse(result.isRunComplete());
    }

    @Test
    void shouldRejectNegativeMetrics() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new ThreadPoolExperimentResult(
                        "RQS",
                        1000,
                        990,
                        0,
                        -1,
                        0,
                        10,
                        500,
                        700,
                        true,
                        true
                )
        );
    }

    @Test
    void shouldRejectBlankStrategyName() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new ThreadPoolExperimentResult(
                        " ",
                        1000,
                        1000,
                        0,
                        0,
                        0,
                        0,
                        500,
                        700,
                        true,
                        true
                )
        );
    }
}