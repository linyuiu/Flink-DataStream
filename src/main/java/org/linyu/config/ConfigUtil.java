package org.linyu.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ConfigUtil {

    private static final Properties PROPERTIES = new Properties();

    static {
        try (InputStream inputStream = ConfigUtil.class
                .getClassLoader()
                .getResourceAsStream("bigdata.properties")) {
            if (inputStream == null)
                throw new RuntimeException("没有找到文件");

            PROPERTIES.load(inputStream);


        } catch (IOException e) {
            throw new RuntimeException("读取配置文件失败", e);
        }
    }

    public static String getString(String key) {
        String value = PROPERTIES.getProperty(key);
        if (value == null) {
            throw new RuntimeException("配置项不存在" + key);
        }
        return value;
    }


    public static String getString(String key, String defaultValue) {
        return PROPERTIES.getProperty(key, defaultValue);
    }

    public static int getInt(String key) {
        return Integer.parseInt(getString(key));
    }

    public static int getInt(String key, int defaultValue) {
        String value = PROPERTIES.getProperty(key);
        return value == null ? defaultValue : Integer.parseInt(value);
    }

    public static long getLong(String key) {
        return Long.parseLong(getString(key));
    }

    public static long getLong(String key, long defaultValue) {
        String value = PROPERTIES.getProperty(key);
        return value == null ? defaultValue : Long.parseLong(value);
    }

    public static boolean getBoolean(String key, boolean defaultValue) {
        String value = PROPERTIES.getProperty(key);
        return value == null ? defaultValue : Boolean.parseBoolean(value);
    }


}
