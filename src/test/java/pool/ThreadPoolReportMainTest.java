package pool;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ThreadPoolReportMainTest {

    @Test
    @Timeout(value = 25, unit = TimeUnit.SECONDS)
    void shouldGenerateCompleteRegressionReports()
            throws Exception {

        Path outputDirectory =
                Paths.get(
                        "target",
                        "thread-pool-report"
                );

        List<ThreadPoolExperimentResult> results =
                ThreadPoolReportMain.generateReports(
                        outputDirectory
                );

        Path csvFile =
                outputDirectory.resolve(
                        "results.csv"
                );

        Path jsonFile =
                outputDirectory.resolve(
                        "results.json"
                );

        assertTrue(
                Files.exists(csvFile),
                "正式CSV报告必须生成"
        );

        assertTrue(
                Files.exists(jsonFile),
                "正式JSON报告必须生成"
        );

        assertEquals(
                8,
                results.size(),
                "正式回归报告必须包含8组实验"
        );

        /*
         * 当前正式回归套件包含：
         *
         * 1. JDK饱和基线
         * 2. ABTP-BISS
         * 3. ABTP-BWS
         * 4. ABTP-RQS
         * 5. Buffer Degree 0.0
         * 6. Buffer Degree 0.5
         * 7. Buffer Degree 1.0
         * 8. 超时强制关闭
         */
        assertEquals(
                8,
                results.size()
        );

        for (ThreadPoolExperimentResult result : results) {
            assertTrue(
                    result.isAccountingComplete(),
                    result.getStrategy()
                            + "必须完整记账"
            );

            assertTrue(
                    result.isTerminated(),
                    result.getStrategy()
                            + "线程池必须最终终止"
            );

            assertTrue(
                    result.isRunComplete(),
                    result.getStrategy()
                            + "实验必须完整结束"
            );
        }

        List<String> csvLines =
                Files.readAllLines(
                        csvFile,
                        StandardCharsets.UTF_8
                );
                
        assertTrue(
                csvLines.stream()
                        .anyMatch(
                                line ->
                                        line.startsWith(
                                                "ABTP-TIMEOUT,"
                                        )
                        ),
                "CSV报告缺少ABTP-TIMEOUT"
        );

        /*
         * 一行表头加八行实验数据。
         */
        assertEquals(
                9,
                csvLines.size()
        );

        assertEquals(
                "strategy,submitted,success,failed,"
                        + "rejected,unresolved,"
                        + "forceEnqueueCount,peakPoolSize,"
                        + "submitMs,totalMs,"
                        + "accounted,terminated,timedOut",
                csvLines.get(0)
        );

        String jsonContent =
                new String(
                        Files.readAllBytes(jsonFile),
                        StandardCharsets.UTF_8
                );

        String[] expectedStrategies = {
                "JDK-SATURATION",
                "ABTP-BISS",
                "ABTP-BWS",
                "ABTP-RQS",
                "ABTP-BUFFER-0.0",
                "ABTP-BUFFER-0.5",
                "ABTP-BUFFER-1.0",
                "ABTP-TIMEOUT"
        };

        for (String strategy : expectedStrategies) {
            assertTrue(
                    jsonContent.contains(
                            "\"strategy\":\""
                                    + strategy
                                    + "\""
                    ),
                    "JSON报告缺少策略：" + strategy
            );

            assertTrue(
                    jsonContent.contains(
                            "\"strategy\":\"ABTP-TIMEOUT\""
                    ),
                    "JSON报告缺少ABTP-TIMEOUT"
            );
        }

        long timedOutCount =
                results.stream()
                        .filter(
                                ThreadPoolExperimentResult::isTimedOut
                        )
                        .count();

        assertEquals(
                1L,
                timedOutCount,
                "只有超时专项实验应标记timedOut=true"
        );

        ThreadPoolExperimentResult timeoutResult =
                results.stream()
                        .filter(
                                result ->
                                        "ABTP-TIMEOUT".equals(
                                                result.getStrategy()
                                        )
                        )
                        .findFirst()
                        .orElseThrow(
                                () -> new AssertionError(
                                        "正式报告缺少ABTP-TIMEOUT"
                                )
                        );

        assertEquals(
                4,
                timeoutResult.getSubmitted()
        );

        assertEquals(
                0,
                timeoutResult.getSuccess()
        );

        assertEquals(
                4,
                timeoutResult.getFailed()
        );

        assertEquals(
                0,
                timeoutResult.getRejected()
        );

        assertEquals(
                0,
                timeoutResult.getUnresolved()
        );

        assertTrue(
                timeoutResult.isAccountingComplete()
        );

        assertTrue(
                timeoutResult.isTerminated()
        );

        assertTrue(
                timeoutResult.isTimedOut()
        );

        ThreadPoolExperimentResult bwsResult =
                results.stream()
                        .filter(
                                result ->
                                        "ABTP-BWS".equals(
                                                result.getStrategy()
                                        )
                        )
                        .findFirst()
                        .orElseThrow(
                                () -> new AssertionError(
                                        "未找到ABTP-BWS结果"
                                )
                        );

        assertEquals(
                0,
                bwsResult.getRejected()
        );

        assertTrue(
                bwsResult.getForceEnqueueCount() > 0
        );

        ThreadPoolExperimentResult bissResult =
                results.stream()
                        .filter(
                                result ->
                                        "ABTP-BISS".equals(
                                                result.getStrategy()
                                        )
                        )
                        .findFirst()
                        .orElseThrow(
                                () -> new AssertionError(
                                        "未找到ABTP-BISS结果"
                                )
                        );

        assertTrue(
                bissResult.getRejected() > 0
        );

        /*
         * 正常场景不能错误标记超时。
         */
        assertFalse(
                bwsResult.isTimedOut()
        );
    }
}