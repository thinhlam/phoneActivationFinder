package com.thinhl.utils;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Random;

public class BuildTestOutputFileUtil {


    public static class CSVUtils {

        private static final char DEFAULT_SEPARATOR = ',';

        public static void writeLine(Writer w, List<String> values) throws IOException {
            writeLine(w, values, DEFAULT_SEPARATOR, ' ');
        }

        public static void writeLine(Writer w, List<String> values, char separators) throws IOException {
            writeLine(w, values, separators, ' ');
        }

        //https://tools.ietf.org/html/rfc4180
        private static String followCVSformat(String value) {

            String result = value;
            if (result.contains("\"")) {
                result = result.replace("\"", "\"\"");
            }
            return result;

        }

        public static void writeLine(Writer w, List<String> values, char separators, char customQuote) throws IOException {

            boolean first = true;

            //default customQuote is empty
            if (separators == ' ') {
                separators = DEFAULT_SEPARATOR;
            }

            StringBuilder sb = new StringBuilder();
            for (String value : values) {
                if (!first) {
                    sb.append(separators);
                }
                if (customQuote == ' ') {
                    sb.append(followCVSformat(value));
                } else {
                    sb.append(customQuote).append(followCVSformat(value)).append(customQuote);
                }

                first = false;
            }
            sb.append("\n");
            w.append(sb.toString());
        }

    }

    private static final String DATE_PATTERN = "YYYY-MM-DD";
    private static final SimpleDateFormat DATE_FORMATTER = new SimpleDateFormat(DATE_PATTERN);

    public static void main(String[] args) {
        String csvFile = "src/main/resources/largeInput.csv";
        try {
            FileWriter writer = new FileWriter(csvFile);
            CSVUtils.writeLine(writer, Arrays.asList("PHONE_NUMBER,ACTIVATION_DATE,DEACTIVATION_DATE"));
            StringBuilder builder = new StringBuilder();
            for (int i = 0; i < 50000000; i++) {
                builder.setLength(0);
                builder.append("0987").append(randomBetween());
                builder.append(",");
                builder.append(getRandomDate(2010, 2012));
                builder.append(",");
                builder.append(getRandomDate(2013, 2017));
                CSVUtils.writeLine(writer, Arrays.asList(builder.toString()));
            }

            writer.flush();
            writer.close();


        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static String getRandomDate(int start, int end) {
        GregorianCalendar gc = new GregorianCalendar();

        int year = randBetween(start, end);

        gc.set(GregorianCalendar.YEAR, year);

        int dayOfYear = randBetween(1, gc.getActualMaximum(GregorianCalendar.DAY_OF_YEAR));

        gc.set(GregorianCalendar.DAY_OF_YEAR, dayOfYear);

        return gc.get(GregorianCalendar.YEAR) + "-" + (gc.get(GregorianCalendar.MONTH) + 1) + "-" + gc.get(GregorianCalendar.DAY_OF_MONTH);
    }

    public static int randBetween(int start, int end) {
        return start + (int) Math.round(Math.random() * (end - start));
    }

    public static int randomBetween() {
        Random r = new Random();
        int Low = 100000;
        int High = 800000;
        return r.nextInt(High - Low) + Low;
    }

}
