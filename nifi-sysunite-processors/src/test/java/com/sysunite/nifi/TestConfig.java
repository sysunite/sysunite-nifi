package com.sysunite.nifi;

import java.io.IOException;
import java.util.Properties;

public class TestConfig {

  public static String getVirtuosoAddress() {
    try {
      Properties p = new Properties();
      p.load(ClassLoader.getSystemResourceAsStream("config.properties"));
      return p.getProperty("VIRTUOSO_ADDRESS");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static String getVirtuosoUser() {
    try {
      Properties p = new Properties();
      p.load(ClassLoader.getSystemResourceAsStream("config.properties"));
      return p.getProperty("VIRTUOSO_USER");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static String getVirtuosoPassword() {
    try {
      Properties p = new Properties();
      p.load(ClassLoader.getSystemResourceAsStream("config.properties"));
      return p.getProperty("VIRTUOSO_PASSWORD");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
