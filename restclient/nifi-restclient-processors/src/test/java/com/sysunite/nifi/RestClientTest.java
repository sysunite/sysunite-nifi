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

/**
 * Created by Jonathan Smit, Sysunite 2016
 */

package com.sysunite.nifi;
//com weaverplatform nifi

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


public class RestClientTest {

    private TestRunner testRunner;

    @Before
    public void init() {

        testRunner = TestRunners.newTestRunner(RestClient.class);

        //InputStream is = new ByteArrayInputStream( inputStr.getBytes() );

        // testRunner.enqueue(is);
    }

    @Test
    public void testOnTrigger() throws IOException {
        // Content to be mock a json file
        InputStream content = new ByteArrayInputStream("{\"hello\":\"nifi rocks\"}".getBytes());

        // Add properties
        testRunner.setProperty(RestClient.URL, "");
        testRunner.setProperty(RestClient.REPORTNAME, "");
        testRunner.setProperty(RestClient.WORKSPACE, "");
        testRunner.setProperty(RestClient.ENTRYCODE, "");


        // Add the content to the runner
        testRunner.enqueue(content);

        // Run the enqueued content, it also takes an int = number of contents queued
        testRunner.run();

        // All results were processed with out failure
        //runner.assertQueueEmpty();

        // If you need to read or do aditional tests on results you can access the content
        List<MockFlowFile> results = testRunner.getFlowFilesForRelationship(RestClient.SUCCESS);
        //assertTrue("1 match", results.size() == 1);

        MockFlowFile result = results.get(0);
        String resultValue = new String(testRunner.getContentAsByteArray(result));
        System.out.println("Match: " + IOUtils.toString(testRunner.getContentAsByteArray(result)));

        // Test attributes and content
        //result.assertAttributeEquals(JsonProcessor.MATCH_ATTR, "nifi rocks");
        //result.assertContentEquals("nifi rocks");

    }

}
