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

import static org.junit.Assert.assertTrue;


public class StringSplitTest {

    private TestRunner testRunner;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(StringSplit.class);
    }

    @Test
    public void testOnTrigger(){
        try {
            String file = "line.txt";

            byte[] contents = FileUtils.readFileToByteArray(new File(getClass().getClassLoader().getResource(file).getFile()));

            InputStream in = new ByteArrayInputStream(contents);

            InputStream cont = new ByteArrayInputStream(IOUtils.toByteArray(in));


            // Add properites
            testRunner.setProperty(StringSplit.SPLIT, ";");
            testRunner.setProperty("RouteA", "0");
            testRunner.setProperty("RouteB", "1");
            testRunner.setProperty("AnotherThing", "3");

            // Add the content to the runner
            testRunner.enqueue(cont);

            // Run the enqueued content, it also takes an int = number of contents queued
            testRunner.run();

            //get contents for a specific dynamic relationship
            List<MockFlowFile> results = testRunner.getFlowFilesForRelationship("RouteB");
            assertTrue("1 match", results.size() == 1);
            MockFlowFile result = results.get(0);
            result.assertAttributeEquals("RouteB", "(AB CT1-N-02A/02B) CT1-W2 hoofdrijbaan");


            //get original flowfile contents
            results = testRunner.getFlowFilesForRelationship("original");
            result = results.get(0);
            String resultValue = new String(testRunner.getContentAsByteArray(result));
            System.out.println(resultValue);

        }catch(IOException e){
            System.out.println("FOUT!!");
            System.out.println(e.getStackTrace());
        }
    }


}
