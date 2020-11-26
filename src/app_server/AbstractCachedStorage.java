package app_server;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.log4j.Logger;

import java.io.*;

/**
 * Base CacheStorage that handles common persist, load and delete methods to handle disk I/Os.
 */
public abstract class AbstractCachedStorage implements CachedStorage {

    private static Logger logger = Logger.getRootLogger();
    private String filename;

    public AbstractCachedStorage(String hostname, int port) {
        this.filename = hostname + String.valueOf(port);
    }

    /**
     * Persists the given key value pair to disk file.
     *
     * @param key   key with which the specified value is to be associated
     * @param value value to be associated with the specified key
     * @return the previous value associated with <tt>key</tt>, or
     * <tt>null</tt> if there was no mapping for <tt>key</tt>.
     * @throws IOException errors in disk read/writes
     */
    public String persistToDisk(String key, String value) throws IOException {

        String workingDirectory = System.getProperty("user.dir");

        String absoluteDirPath = workingDirectory + File.separator +filename;
        File dir = new File(absoluteDirPath);

        dir.mkdir();

        String absoluteFilePath = absoluteDirPath + File.separator + "Data";
        File inputFile = new File(absoluteFilePath);
        try {
            inputFile.createNewFile();
        } catch (IOException e) {
            logger.error("Cannot create or read data file.");
            throw e;
        }

        BufferedReader reader;
        try {
            reader = new BufferedReader(new FileReader(inputFile));
        } catch (IOException e) {
            logger.error("Cannot create reader for data file");
            throw e;
        }

        File tempFile = new File(absoluteDirPath + File.pathSeparator + "temp");
        BufferedWriter writer;
        try {
            writer = new BufferedWriter(new FileWriter(tempFile));
        } catch (IOException e) {
            logger.error("Cannot create writer for temporary file.");
            throw e;
        }

        String rtn = null;
        try {
            String currentLine;
            while ((currentLine = reader.readLine()) != null) {
                String trimmedLine = currentLine.trim();
                CSVParser parser = CSVParser.parse(trimmedLine, CSVFormat.RFC4180);
                CSVRecord csvRecord = parser.getRecords().get(0);
                if (csvRecord.get(0).equals(key)) {
                    rtn = csvRecord.get(1);
                    continue;
                }
                writer.write(currentLine + System.getProperty("line.separator"));
            }
            String escapedKey = StringEscapeUtils.escapeCsv(key);
            String escapedValue = StringEscapeUtils.escapeCsv(value);
            String newLine = escapedKey + "," + escapedValue;
            writer.write(newLine + System.getProperty("line.separator"));
            writer.close();
            reader.close();
        } catch (IOException e) {
            logger.error("Error while copying data to temporary file.");
            tempFile.delete();
            throw e;
        }

        if (!tempFile.renameTo(inputFile)) {
            tempFile.delete();
            rtn = null;
        }
        return rtn;
    }

    public void persistWithoutUniqueCheck(String key, String value) {
        String workingDirectory = System.getProperty("user.dir");

        String absoluteDirPath = workingDirectory + File.separator +filename;
        File dir = new File(absoluteDirPath);

        dir.mkdir();
        String absoluteFilePath = absoluteDirPath + File.separator + "Data";
        File dataFile = new File(absoluteFilePath);

        try {
            dataFile.createNewFile();
        } catch (IOException e) {
            logger.error("Cannot create or read data file.");
        }

        BufferedWriter writer = null;
        try {
            writer = new BufferedWriter(new FileWriter(dataFile, true));
        } catch (IOException e) {
            logger.error("Cannot create writer for temporary file.");
        }

        String escapedKey = StringEscapeUtils.escapeCsv(key);
        String escapedValue = StringEscapeUtils.escapeCsv(value);
        String newLine = escapedKey + "," + escapedValue;
        try {
            writer.write(newLine + System.getProperty("line.separator"));
            writer.close();
        } catch (IOException e) {
            logger.error("Error writing to data file.");
        }

    }

    /**
     * Loads the value associated with the given key from disk file.
     *
     * @param key key of the key-value pair in which the value is to be retrieved
     * @return value currently on disk of the input key
     * @throws IOException errors in disk read/writes
     */
    public String loadFromDisk(String key) throws IOException {

        String workingDirectory = System.getProperty("user.dir");

        String absoluteDirPath = workingDirectory + File.separator +filename;
        File dir = new File(absoluteDirPath);

        dir.mkdir();

        String absoluteFilePath = absoluteDirPath + File.separator + "Data";

        File inputFile = new File(absoluteFilePath);
        if (!inputFile.exists()) {
            return null;
        }

        BufferedReader reader;
        try {
            reader = new BufferedReader(new FileReader(inputFile));
        } catch (IOException e) {
            logger.error("Cannot create reader for data file");
            throw e;
        }

        String value = null;
        try {
            String currentLine;
            while ((currentLine = reader.readLine()) != null) {
                String trimmedLine = currentLine.trim();
                CSVParser parser = CSVParser.parse(trimmedLine, CSVFormat.RFC4180);
                CSVRecord csvRecord = parser.getRecords().get(0);
                if (csvRecord.get(0).equals(key)) {
                    value = csvRecord.get(1);
                    break;
                }
            }
            reader.close();
        } catch (IOException e) {
            logger.error("Error reading date file.");
            throw e;
        }
        return value;
    }

    /**
     * Deletes the key value pair referenced by the input key from disk file.
     *
     * @param key key in which the key value pair should be deleted from disk
     * @return the value that was deleted
     * @throws IOException errors in disk read/writes
     */
    public String deleteFromDisk(String key) throws IOException {
        String workingDirectory = System.getProperty("user.dir");

        String absoluteDirPath = workingDirectory + File.separator +filename;
        File dir = new File(absoluteDirPath);

        dir.mkdir();

        String absoluteFilePath = absoluteDirPath + File.separator + "Data";

        File inputFile = new File(absoluteFilePath);
        if (!inputFile.exists()) {
            return null;
        }

        BufferedReader reader;
        try {
            reader = new BufferedReader(new FileReader(inputFile));
        } catch (IOException e) {
            logger.error("Cannot create reader for data file");
            throw e;
        }

        File tempFile = new File(absoluteDirPath + File.separator + "temp");
        BufferedWriter writer;
        try {
            writer = new BufferedWriter(new FileWriter(tempFile));
        } catch (IOException e) {
            logger.error("Cannot create writer for temporary file.");
            throw e;
        }

        String rtn = null;
        try {
            String currentLine;
            while ((currentLine = reader.readLine()) != null) {
                String trimmedLine = currentLine.trim();
                CSVParser parser = CSVParser.parse(trimmedLine, CSVFormat.RFC4180);
                CSVRecord csvRecord = parser.getRecords().get(0);
                if (csvRecord.get(0).equals(key)) {
                    rtn = csvRecord.get(1);
                    continue;
                }
                writer.write(currentLine + System.getProperty("line.separator"));
            }
            writer.close();
            reader.close();
        } catch (IOException e) {
            logger.error("Error while copying data to temporary file.");
            tempFile.delete();
            throw e;
        }

        if (!tempFile.renameTo(inputFile)) {
            tempFile.delete();
            rtn = null;
        }
        return rtn;
    }
}
