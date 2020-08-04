package bezyan.data.tweetingester.common;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class PropertiesUtil {
    public static Properties getPropertiesFromFile(String relativeFilePath) {
        FileReader reader = null;
        try {
            reader = new FileReader(relativeFilePath);
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Unable to read properties file", e);
        }

        Properties p = new Properties();
        try {
            p.load(reader);
        } catch (IOException e) {
            throw new RuntimeException("Unable to load properties from file", e);
        }

        return p;
    }
}
