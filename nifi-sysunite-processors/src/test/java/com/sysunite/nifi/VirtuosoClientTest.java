package com.sysunite.nifi;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public class VirtuosoClientTest {

  private TestRunner testRunner;

  @Before
  public void init() {
    testRunner = TestRunners.newTestRunner(VirtuosoClient.class);
  }

  @Test
  public void testOnTrigger() throws IOException {

    // Content to be mock a json file
    InputStream content = new ByteArrayInputStream("".getBytes());

    // Add properties
    testRunner.setProperty(VirtuosoClient.ADDRESS, "127.0.0.1:1111");
    testRunner.setProperty(VirtuosoClient.USER, "dba");
    testRunner.setProperty(VirtuosoClient.PASSWORD, "ibdk2015");
    testRunner.setProperty(VirtuosoClient.SELECT, "s,p,o");
    testRunner.setProperty(VirtuosoClient.SEPARATOR, "\t");
    testRunner.setProperty(VirtuosoClient.QUERY, "SELECT ?s ?p ?o WHERE {?s ?p ?o}");

    testRunner.setProperty("olsw", "http://www.openlinksw.com/virtrdf-data-formats#");
    testRunner.setProperty("virt", "http://www.openlinksw.com/schemas/virtrdf#");
    testRunner.setProperty("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#");


    // Add the content to the runner
    testRunner.enqueue(content);

    // Run the enqueued content, it also takes an int = number of contents queued
    testRunner.run();



    // If you need to read or do additional tests on results you can access the content
    List<MockFlowFile> results = testRunner.getFlowFilesForRelationship(RestClient.SUCCESS);
    for(MockFlowFile res : results) {
      System.out.println("Match: " + IOUtils.toString(testRunner.getContentAsByteArray(res)));

    }
  }
}