package pool;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

/**
 * 线程池实验报告生成器。
 *
 * 将ThreadPoolExperimentResult输出为CSV或JSON文件。
 * 本类只负责结果序列化，不负责执行实验。
 */
public final class ThreadPoolExperimentReportWriter {

    private static final String CSV_HEADER =
            "strategy,submitted,success,failed,"
                    + "rejected,unresolved,"
                    + "forceEnqueueCount,peakPoolSize,"
                    + "submitMs,totalMs,"
                    + "accounted,terminated,timedOut";

    private ThreadPoolExperimentReportWriter() {
        // 工具类不允许实例化
    }

    /**
     * 将实验结果写入CSV文件。
     *
     * @param outputFile 输出文件路径
     * @param results    实验结果集合
     */
    public static void writeCsv(
            Path outputFile,
            List<ThreadPoolExperimentResult> results)
            throws IOException {

        validateArguments(outputFile, results);
        createParentDirectory(outputFile);

        try (BufferedWriter writer =
                     Files.newBufferedWriter(
                             outputFile,
                             StandardCharsets.UTF_8,
                             StandardOpenOption.CREATE,
                             StandardOpenOption.TRUNCATE_EXISTING,
                             StandardOpenOption.WRITE
                     )) {

            writer.write(CSV_HEADER);

            for (ThreadPoolExperimentResult result : results) {
                writer.newLine();
                writer.write(toCsvLine(result));
            }
        }
    }

    /**
     * 将实验结果写入JSON数组文件。
     *
     * @param outputFile 输出文件路径
     * @param results    实验结果集合
     */
    public static void writeJson(
            Path outputFile,
            List<ThreadPoolExperimentResult> results)
            throws IOException {

        validateArguments(outputFile, results);
        createParentDirectory(outputFile);

        try (BufferedWriter writer =
                     Files.newBufferedWriter(
                             outputFile,
                             StandardCharsets.UTF_8,
                             StandardOpenOption.CREATE,
                             StandardOpenOption.TRUNCATE_EXISTING,
                             StandardOpenOption.WRITE
                     )) {

            writer.write("[");

            for (int index = 0;
                 index < results.size();
                 index++) {

                ThreadPoolExperimentResult result =
                        results.get(index);

                writer.newLine();
                writer.write("  ");
                writer.write(toJsonObject(result));

                if (index < results.size() - 1) {
                    writer.write(",");
                }
            }

            if (!results.isEmpty()) {
                writer.newLine();
            }

            /*
             * 不在右方括号后追加换行，
             * 以满足JSON测试中的endsWith("]")断言。
             */
            writer.write("]");
        }
    }

    /**
     * 生成一行CSV数据。
     */
    private static String toCsvLine(
            ThreadPoolExperimentResult result) {

        StringBuilder line = new StringBuilder();

        appendCsvValue(
                line,
                result.getStrategy()
        );
        appendCsvValue(
                line,
                result.getSubmitted()
        );
        appendCsvValue(
                line,
                result.getSuccess()
        );
        appendCsvValue(
                line,
                result.getFailed()
        );
        appendCsvValue(
                line,
                result.getRejected()
        );
        appendCsvValue(
                line,
                result.getUnresolved()
        );
        appendCsvValue(
                line,
                result.getForceEnqueueCount()
        );
        appendCsvValue(
                line,
                result.getPeakPoolSize()
        );
        appendCsvValue(
                line,
                result.getSubmitMs()
        );
        appendCsvValue(
                line,
                result.getTotalMs()
        );
        appendCsvValue(
                line,
                result.isAccounted()
        );
        appendCsvValue(
                line,
                result.isTerminated()
        );
        appendCsvValue(
                line,
                result.isTimedOut()
        );

        return line.toString();
    }

    /**
     * 向CSV行追加一个字段。
     */
    private static void appendCsvValue(
            StringBuilder line,
            Object value) {

        if (line.length() > 0) {
            line.append(',');
        }

        line.append(
                escapeCsv(
                        String.valueOf(value)
                )
        );
    }

    /**
     * 转义CSV字符串。
     *
     * 字段包含逗号、双引号或换行时：
     * 1. 使用双引号包围整个字段；
     * 2. 原有双引号替换为两个双引号。
     */
    private static String escapeCsv(
            String value) {

        boolean requiresQuotes =
                value.indexOf(',') >= 0
                        || value.indexOf('"') >= 0
                        || value.indexOf('\n') >= 0
                        || value.indexOf('\r') >= 0;

        if (!requiresQuotes) {
            return value;
        }

        return "\""
                + value.replace("\"", "\"\"")
                + "\"";
    }

    /**
     * 将一个实验结果转换为JSON对象。
     */
    private static String toJsonObject(
            ThreadPoolExperimentResult result) {

        StringBuilder json = new StringBuilder();

        json.append('{');

        appendJsonStringField(
                json,
                "strategy",
                result.getStrategy()
        );

        appendJsonNumberField(
                json,
                "submitted",
                result.getSubmitted()
        );

        appendJsonNumberField(
                json,
                "success",
                result.getSuccess()
        );

        appendJsonNumberField(
                json,
                "failed",
                result.getFailed()
        );

        appendJsonNumberField(
                json,
                "rejected",
                result.getRejected()
        );

        appendJsonNumberField(
                json,
                "unresolved",
                result.getUnresolved()
        );

        appendJsonNumberField(
                json,
                "forceEnqueueCount",
                result.getForceEnqueueCount()
        );

        appendJsonNumberField(
                json,
                "peakPoolSize",
                result.getPeakPoolSize()
        );

        appendJsonNumberField(
                json,
                "submitMs",
                result.getSubmitMs()
        );

        appendJsonNumberField(
                json,
                "totalMs",
                result.getTotalMs()
        );

        appendJsonBooleanField(
                json,
                "accounted",
                result.isAccounted()
        );

        appendJsonBooleanField(
                json,
                "terminated",
                result.isTerminated()
        );

        appendJsonBooleanField(
                json,
                "timedOut",
                result.isTimedOut()
        );

        json.append('}');

        return json.toString();
    }

    /**
     * 追加JSON字符串字段。
     */
    private static void appendJsonStringField(
            StringBuilder json,
            String fieldName,
            String fieldValue) {

        appendJsonSeparator(json);

        json.append('"')
                .append(escapeJson(fieldName))
                .append("\":\"")
                .append(escapeJson(fieldValue))
                .append('"');
    }

    /**
     * 追加JSON数值字段。
     */
    private static void appendJsonNumberField(
            StringBuilder json,
            String fieldName,
            long fieldValue) {

        appendJsonSeparator(json);

        json.append('"')
                .append(escapeJson(fieldName))
                .append("\":")
                .append(fieldValue);
    }

    /**
     * 追加JSON布尔字段。
     */
    private static void appendJsonBooleanField(
            StringBuilder json,
            String fieldName,
            boolean fieldValue) {

        appendJsonSeparator(json);

        json.append('"')
                .append(escapeJson(fieldName))
                .append("\":")
                .append(fieldValue);
    }

    /**
     * 当前JSON对象已有字段时追加逗号。
     */
    private static void appendJsonSeparator(
            StringBuilder json) {

        if (json.length() > 1) {
            json.append(',');
        }
    }

    /**
     * JSON字符串转义。
     */
    private static String escapeJson(
            String value) {

        StringBuilder escaped =
                new StringBuilder();

        for (int index = 0;
             index < value.length();
             index++) {

            char character =
                    value.charAt(index);

            switch (character) {
                case '"':
                    escaped.append("\\\"");
                    break;

                case '\\':
                    escaped.append("\\\\");
                    break;

                case '\b':
                    escaped.append("\\b");
                    break;

                case '\f':
                    escaped.append("\\f");
                    break;

                case '\n':
                    escaped.append("\\n");
                    break;

                case '\r':
                    escaped.append("\\r");
                    break;

                case '\t':
                    escaped.append("\\t");
                    break;

                default:
                    if (character < 0x20) {
                        escaped.append(
                                String.format(
                                        "\\u%04x",
                                        (int) character
                                )
                        );
                    } else {
                        escaped.append(character);
                    }
            }
        }

        return escaped.toString();
    }

    /**
     * 输出文件父目录不存在时自动创建。
     */
    private static void createParentDirectory(
            Path outputFile)
            throws IOException {

        Path parentDirectory =
                outputFile.getParent();

        if (parentDirectory != null) {
            Files.createDirectories(
                    parentDirectory
            );
        }
    }

    /**
     * 进行基础参数校验。
     */
    private static void validateArguments(
            Path outputFile,
            List<ThreadPoolExperimentResult> results) {

        if (outputFile == null) {
            throw new IllegalArgumentException(
                    "outputFile must not be null"
            );
        }

        if (results == null) {
            throw new IllegalArgumentException(
                    "results must not be null"
            );
        }

        for (int index = 0;
             index < results.size();
             index++) {

            if (results.get(index) == null) {
                throw new IllegalArgumentException(
                        "results must not contain null element, "
                                + "index=" + index
                );
            }
        }
    }
}