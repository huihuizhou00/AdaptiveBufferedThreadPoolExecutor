package pool;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ThreadPoolExperimentReportWriterTest {

    @TempDir
    Path tempDirectory;

    @Test
    void shouldWriteCompleteCsvReport()
            throws IOException {

        List<ThreadPoolExperimentResult> results =
                createSampleResults();

        Path csvFile =
                tempDirectory.resolve(
                        "thread-pool-results.csv"
                );

        ThreadPoolExperimentReportWriter.writeCsv(
                csvFile,
                results
        );

        assertTrue(
                Files.exists(csvFile),
                "CSV文件必须被创建"
        );

        List<String> lines =
                Files.readAllLines(
                        csvFile,
                        StandardCharsets.UTF_8
                );

        assertEquals(
                3,
                lines.size(),
                "CSV应包含1行表头和2行实验数据"
        );

        assertEquals(
                "strategy,submitted,success,failed,"
                        + "rejected,unresolved,"
                        + "forceEnqueueCount,peakPoolSize,"
                        + "submitMs,totalMs,"
                        + "accounted,terminated,timedOut",
                lines.get(0)
        );

        assertEquals(
                "ABTP-BWS,12,12,0,0,0,"
                        + "10,1,803,963,"
                        + "true,true,false",
                lines.get(1)
        );

        assertEquals(
                "ABTP-TIMEOUT,4,0,4,0,0,"
                        + "0,1,0,2001,"
                        + "true,true,true",
                lines.get(2)
        );
    }

    @Test
    void shouldWriteCompleteJsonReport()
            throws IOException {

        List<ThreadPoolExperimentResult> results =
                createSampleResults();

        Path jsonFile =
                tempDirectory.resolve(
                        "thread-pool-results.json"
                );

        ThreadPoolExperimentReportWriter.writeJson(
                jsonFile,
                results
        );

        assertTrue(
                Files.exists(jsonFile),
                "JSON文件必须被创建"
        );

        String content =
                new String(
                        Files.readAllBytes(jsonFile),
                        StandardCharsets.UTF_8
                );

        assertTrue(
                content.startsWith("["),
                "JSON报告应以数组开始"
        );

        assertTrue(
                content.endsWith("]"),
                "JSON报告应以数组结束"
        );

        assertTrue(
                content.contains(
                        "\"strategy\":\"ABTP-BWS\""
                )
        );

        assertTrue(
                content.contains(
                        "\"success\":12"
                )
        );

        assertTrue(
                content.contains(
                        "\"forceEnqueueCount\":10"
                )
        );

        assertTrue(
                content.contains(
                        "\"peakPoolSize\":1"
                )
        );

        assertTrue(
                content.contains(
                        "\"timedOut\":false"
                )
        );

        assertTrue(
                content.contains(
                        "\"strategy\":\"ABTP-TIMEOUT\""
                )
        );

        assertTrue(
                content.contains(
                        "\"failed\":4"
                )
        );

        assertTrue(
                content.contains(
                        "\"timedOut\":true"
                )
        );
    }

    private static List<ThreadPoolExperimentResult>
            createSampleResults() {

        ThreadPoolExperimentResult bws =
                new ThreadPoolExperimentResult(
                        "ABTP-BWS",
                        12,
                        12,
                        0,
                        0,
                        0,
                        10,
                        1,
                        803L,
                        963L,
                        true,
                        true,
                        false
                );

        ThreadPoolExperimentResult timeout =
                new ThreadPoolExperimentResult(
                        "ABTP-TIMEOUT",
                        4,
                        0,
                        4,
                        0,
                        0,
                        0,
                        1,
                        0L,
                        2001L,
                        true,
                        true,
                        true
                );

        return Arrays.asList(
                bws,
                timeout
        );
    }
}