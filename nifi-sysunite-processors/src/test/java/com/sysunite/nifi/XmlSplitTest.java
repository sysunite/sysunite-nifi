/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.sysunite.nifi;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;


public class XmlSplitTest {

  private TestRunner testRunner;

  @Before
  public void init() {
    testRunner = TestRunners.newTestRunner(XmlSplit.class);
  }

  @Test
  public void testOnTrigger(){

    try {


      //import a xml file and simulate it as a flowfile earlier used with some attributes.
      String file = "slagboom.xml";
      byte[] contents = FileUtils.readFileToByteArray(new File(getClass().getClassLoader().getResource(file).getFile()));
      InputStream in = new ByteArrayInputStream(contents);
      InputStream cont = new ByteArrayInputStream(IOUtils.toByteArray(in));
      ProcessSession processSession = testRunner.getProcessSessionFactory().createSession();
      FlowFile f = processSession.create();
      f = processSession.importFrom(cont, f);
      f = processSession.putAttribute(f, "RandomId", "123");


      // Add properites
      // Add properites
      //testRunner.setProperty("FunctionalPhysicalObject-id", "/FunctionalPhysicalObject/@id");
      //testRunner.setProperty("BeginOfLife-id", "/FunctionalPhysicalObject/BeginOfLife/@value");

      testRunner.setProperty("HasAsSubject", "/FunctionalPhysicalObject/HasAsSubject");
      //testRunner.setProperty("IsMaterializedBy", "/FunctionalPhysicalObject/IsMaterializedBy");


      testRunner.removeProperty(new PropertyDescriptor.Builder().name("HasAsSubject").build());

      // Add the content to the runner
      testRunner.enqueue(f);

      // Run the enqueued content, it also takes an int = number of contents queued
      testRunner.run();

//            //get contents for original relationship
//            List<MockFlowFile> results = testRunner.getFlowFilesForRelationship("original");
//            assertTrue("1 match", results.size() == 1);
//            MockFlowFile result = results.get(0);
//            String resultValue = new String(testRunner.getContentAsByteArray(result));
//            System.out.println(resultValue);


//            List<MockFlowFile> results3 = testRunner.getFlowFilesForRelationship("FunctionalPhysicalObject-id");
//            MockFlowFile result3 = results3.get(0);
//            result3.assertAttributeExists("FunctionalPhysicalObject-id");
//            String xmlValue3 = result3.getAttribute("FunctionalPhysicalObject-id");
//            System.out.println("-----------");
//            System.out.println(xmlValue3);
//
//            //get contents for a specific dynamic relationship
//            List<MockFlowFile> results = testRunner.getFlowFilesForRelationship("BeginOfLife");
//            MockFlowFile result = results.get(0);
//            result.assertAttributeExists("BeginOfLife");
//            String xmlValue = result.getAttribute("BeginOfLife");
//            System.out.println("-----------");
//            System.out.println(xmlValue);
//
//
//            List<MockFlowFile> results2 = testRunner.getFlowFilesForRelationship("IsMaterializedBy");
//            MockFlowFile result2 = results2.get(0);
//            result2.assertAttributeExists("IsMaterializedBy");
//            String xmlValue2 = result2.getAttribute("IsMaterializedBy");
//            System.out.println("-----------");
//            System.out.println(xmlValue2);

      //if(testRunner.getProcessContext().getAvailableRelationships().contains(new PropertyDescriptor.Builder().name("HasAsSubject").build()
      List<MockFlowFile> results2 = testRunner.getFlowFilesForRelationship("HasAsSubject");

      for(MockFlowFile mockf : results2) {
        //MockFlowFile result2 = results2.get(0);
        //f.assertAttributeExists("HasAsSubject");
        String xmlValue2 = f.getAttribute("RandomId");
        System.out.println("-----------");
        System.out.println(xmlValue2);
        String resultValue = new String(testRunner.getContentAsByteArray(mockf));
        System.out.println(resultValue);
      }


    }catch(IOException e){
      System.out.println("FOUT!!");
      System.out.println(e.getStackTrace());
    }
  }

}
