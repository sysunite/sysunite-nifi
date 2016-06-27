package com.sysunite.nifi;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;
import virtuoso.jena.driver.ISQLChannel;
import virtuoso.jena.driver.Virtuoso;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public class VirtuosoClientTest {

  private TestRunner testRunner;

  /**
   *
   *  -> Run the docker tenforce/virtuoso:1.0.0-virtuoso7.2.2 image before running this test
   *  -> Set properties in config.properties
   *
   */



  @Before
  public void init() {

    Virtuoso quadStore = new Virtuoso("jdbc:virtuoso://"+TestConfig.getVirtuosoAddress(), TestConfig.getVirtuosoUser(), TestConfig.getVirtuosoPassword());
    ISQLChannel.sendQuery(quadStore.getVirtGraph(), "DELETE FROM DB.DBA.RDF_QUAD");

    ISQLChannel.sendQuery(quadStore.getVirtGraph(), "sparql INSERT DATA INTO GRAPH <x> {  <http://ba#a> <b> \"dev\" . } ");
    ISQLChannel.sendQuery(quadStore.getVirtGraph(), "sparql INSERT DATA INTO GRAPH <y> {  <c> <d> <e> . } ");


    testRunner = TestRunners.newTestRunner(VirtuosoClient.class);
  }

  @Test
  public void testOnTrigger() throws IOException {

    // Content to be mock a json file
    InputStream content = new ByteArrayInputStream("".getBytes());

    // Add properties
    testRunner.setProperty(VirtuosoClient.ADDRESS, TestConfig.getVirtuosoAddress());
    testRunner.setProperty(VirtuosoClient.USER, TestConfig.getVirtuosoUser());
    testRunner.setProperty(VirtuosoClient.PASSWORD, TestConfig.getVirtuosoPassword());
    testRunner.setProperty(VirtuosoClient.SELECT, "s,p,o");
    testRunner.setProperty(VirtuosoClient.SEPARATOR, "\t");
    testRunner.setProperty(VirtuosoClient.QUERY, "SELECT ?s ?p ?o WHERE {?s ?p ?o}");

    testRunner.setProperty("ba", "http://ba#");

    // Add the content to the runner
    testRunner.enqueue(content);

    // Run the enqueued content, it also takes an int = number of contents queued
    testRunner.run();

    // If you need to read or do additional tests on results you can access the content
    List<MockFlowFile> results = testRunner.getFlowFilesForRelationship(RestClient.SUCCESS);
    assert(IOUtils.toString(testRunner.getContentAsByteArray(results.get(0))).equals("ba:a\tb\tdev"));
    assert(IOUtils.toString(testRunner.getContentAsByteArray(results.get(1))).equals("c\td\te"));
  }
}