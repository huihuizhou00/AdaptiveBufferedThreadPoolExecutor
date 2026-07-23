package pool;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 线程池回归实验正式入口。
 *
 * 运行一组快速、可重复的实验，并将结果输出为：
 *
 * target/thread-pool-report/results.csv
 * target/thread-pool-report/results.json
 *
 * 本入口用于自动化回归和CI报告归档，
 * 不替代长时间性能基准实验。
 */
public final class ThreadPoolReportMain {

    private ThreadPoolReportMain() {
        // 工具入口类不允许实例化
    }

    public static void main(String[] args)
            throws Exception {

        Path outputDirectory =
                resolveOutputDirectory(args);

        List<ThreadPoolExperimentResult> results =
                generateReports(outputDirectory);

        System.out.println(
                "线程池回归报告生成完成："
                        + outputDirectory.toAbsolutePath()
        );

        for (ThreadPoolExperimentResult result : results) {
            System.out.println(
                    "[REPORT] " + result
            );
        }
    }

    /**
     * 执行正式回归套件并写入CSV、JSON。
     */
    public static List<ThreadPoolExperimentResult>
            generateReports(Path outputDirectory)
            throws IOException, InterruptedException {

        if (outputDirectory == null) {
            throw new IllegalArgumentException(
                    "outputDirectory must not be null"
            );
        }

        List<ThreadPoolExperimentResult> results =
                new ArrayList<>();

        /*
         * 1. JDK饱和基线
         */
        results.add(
                ThreadPoolExperimentRunner
                        .runJdkExperiment(
                                "JDK-SATURATION",
                                1,
                                1,
                                1,
                                12,
                                80L,
                                0L,
                                10L
                        )
        );

        /*
         * 2. BISS短退避策略
         */
        results.add(
                runAbtpSaturationStrategy(
                        "ABTP-BISS",
                        -1,
                        1.0
                )
        );

        /*
         * 3. BWS限时阻塞策略
         */
        results.add(
                runAbtpSaturationStrategy(
                        "ABTP-BWS",
                        100,
                        1.0
                )
        );

        /*
         * 4. RQS单次非阻塞重试
         */
        results.add(
                runAbtpSaturationStrategy(
                        "ABTP-RQS",
                        -1,
                        -1.0
                )
        );

        /*
         * 5～7. Buffer Degree扩容水位
         */
        results.add(
                runBufferDegreeScenario(
                        "ABTP-BUFFER-0.0",
                        0.0
                )
        );

        results.add(
                runBufferDegreeScenario(
                        "ABTP-BUFFER-0.5",
                        0.5
                )
        );

        results.add(
                runBufferDegreeScenario(
                        "ABTP-BUFFER-1.0",
                        1.0
                )
        );

        /*
         * 8. 超时强制关闭专项
         */
        results.add(
                runTimeoutScenario()
        );

        Path csvFile =
                outputDirectory.resolve(
                        "results.csv"
                );

        Path jsonFile =
                outputDirectory.resolve(
                        "results.json"
                );

        ThreadPoolExperimentReportWriter.writeCsv(
                csvFile,
                results
        );

        ThreadPoolExperimentReportWriter.writeJson(
                jsonFile,
                results
        );

        /*
         * 防止调用方修改已经写入报告的结果集合。
         */
        return Collections.unmodifiableList(
                new ArrayList<>(results)
        );
    }

    /**
     * 运行一种ABTP饱和策略。
     */
    private static ThreadPoolExperimentResult
            runAbtpSaturationStrategy(
                    String strategyName,
                    int threadLoadJudge,
                    double cpuLoadJudge)
            throws InterruptedException {

        return ThreadPoolExperimentRunner
                .runAbtpExperiment(
                        strategyName,
                        1,
                        1,
                        1,
                        12,
                        80L,
                        0L,
                        1.0,
                        true,
                        threadLoadJudge,
                        cpuLoadJudge,
                        10L,
                        1000L,
                        3,
                        10L
                );
    }

    /**
     * 运行一种Buffer Degree扩容场景。
     */
    private static ThreadPoolExperimentResult
            runBufferDegreeScenario(
                    String strategyName,
                    double bufferDegree)
            throws InterruptedException {

        return ThreadPoolExperimentRunner
                .runAbtpExperiment(
                        strategyName,
                        1,
                        4,
                        4,
                        6,
                        300L,
                        0L,
                        bufferDegree,
                        false,
                        -1,
                        -1.0,
                        10L,
                        100L,
                        3,
                        5L
                );
    }

    /**
     * 运行超时强制关闭专项。
     */
    private static ThreadPoolExperimentResult
            runTimeoutScenario()
            throws InterruptedException {

        return ThreadPoolExperimentRunner
                .runAbtpExperiment(
                        "ABTP-TIMEOUT",
                        1,
                        1,
                        3,
                        4,
                        5000L,
                        0L,
                        1.0,
                        false,
                        -1,
                        -1.0,
                        10L,
                        100L,
                        3,
                        1L
                );
    }

    /**
     * 命令行传入目录时使用指定目录；
     * 未传入时使用默认target目录。
     */
    private static Path resolveOutputDirectory(
            String[] args) {

        if (args != null
                && args.length > 0
                && args[0] != null
                && !args[0].trim().isEmpty()) {

            return Paths.get(args[0]);
        }

        return Paths.get(
                "target",
                "thread-pool-report"
        );
    }
}