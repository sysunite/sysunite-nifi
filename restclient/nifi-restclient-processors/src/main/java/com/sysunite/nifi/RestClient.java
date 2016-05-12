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

import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.*;

@Tags({"rest-client, relatics"})
@CapabilityDescription("A simple rest-client for relatics")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
public class RestClient extends AbstractProcessor {

  //parameter definitions for nifi-component
  public static final PropertyDescriptor URL = new PropertyDescriptor
    .Builder().name("url")
    .description("Soap endpoint")
    .required(true)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build();

  public static final PropertyDescriptor REPORTNAME = new PropertyDescriptor
    .Builder().name("reportname")
    .description("Relatics GetResult operation")
    .required(true)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build();

  public static final PropertyDescriptor WORKSPACE = new PropertyDescriptor
    .Builder().name("workspace")
    .description("Relatics workspace")
    .required(true)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build();

  public static final PropertyDescriptor ENTRYCODE = new PropertyDescriptor
    .Builder().name("entrycode")
    .description("Relatics entrycode")
    .required(true)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build();

  //component relation definitions
  public static final Relationship SUCCESS = new Relationship.Builder()
    .name("success")
    .description("Relationship to transfer data to.")
    .build();

  private List<PropertyDescriptor> descriptors;

  private Set<Relationship> relationships;

  private ProcessorLog logger;

  @Override
  protected void init(final ProcessorInitializationContext context) {

    //init properties
    final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
    descriptors.add(URL);
    descriptors.add(REPORTNAME);
    descriptors.add(WORKSPACE);
    descriptors.add(ENTRYCODE);
    this.descriptors = Collections.unmodifiableList(descriptors);

    //init relations
    final Set<Relationship> relationships = new HashSet<Relationship>();
    relationships.add(SUCCESS);
    this.relationships = Collections.unmodifiableSet(relationships);

    //init logger
    this.logger = context.getLogger();
  }

  @Override
  public Set<Relationship> getRelationships() {
    return this.relationships;
  }

  @Override
  public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
    return descriptors;
  }

  @OnScheduled
  public void onScheduled(final ProcessContext context) {

  }

  //this method is triggered when nifi-component start.
  @Override
  public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

    //create new flowfile
    FlowFile flowfile = session.create();

    //get relatics data as inputstream-object
    InputStream data = fetch(
      getSoapEnvelope(
        context.getProperty(REPORTNAME).getValue(),
        context.getProperty(WORKSPACE).getValue(),
        context.getProperty(ENTRYCODE).getValue()
      ),
      context.getProperty(URL).getValue()
    );


    //add inputstream-object to flowfile
    flowfile = session.importFrom(data, flowfile);

    //make flowfile as input for another processor bounded to this relation
    session.transfer(flowfile, SUCCESS);

  }

  //creates xml-soap-object within string-object
  public String getSoapEnvelope(String reportName, String workspace, String entryCode){

    String rt = "";

    rt = "<s:Envelope xmlns:s=\"http://www.w3.org/2003/05/soap-envelope\">\n" +
      "  <s:Body>\n" +
      "    <GetResult xmlns=\"http://www.relatics.com/\">\n" +
      "      <Operation>" + reportName + "</Operation>\n" +
      "      <Identification>\n" +
      "        <Relatics:Identification xmlns:Relatics=\"http://www.relatics.com/\">\n" +
      "          <Relatics:Workspace>"+ workspace +"</Relatics:Workspace>\n" +
      "        </Relatics:Identification>\n" +
      "      </Identification>\n" +
      "      <Parameters>\n" +
      "        <Relatics:Parameters xmlns:Relatics=\"http://www.relatics.com/\"/>\n" +
      "      </Parameters>\n" +
      "      <Authentication>\n" +
      "        <Relatics:Authentication xmlns:Relatics=\"http://www.relatics.com/\">\n" +
      "          <Relatics:Entrycode>" + entryCode + "</Relatics:Entrycode>\n" +
      "        </Relatics:Authentication>\n" +
      "      </Authentication>\n" +
      "    </GetResult>\n" +
      "  </s:Body>\n" +
      "</s:Envelope>";

    return rt;
  }

  //makes connection to http-xml-soap-endpoint, request relatics data with soap-envelope and returns it as inputstream
  public InputStream fetch(String soapEnvelope, String aHttpURLConnection) {
    try {

      HttpURLConnection connection = (HttpURLConnection) new URL(aHttpURLConnection).openConnection();

      connection.setDoOutput(true);

      connection.setRequestMethod("POST");
      connection.setRequestProperty("Content-Type", "application/soap+xml; charset=utf-8; action=\"http://www.relatics.com/GetResult\"");
      connection.setRequestProperty("Content-Length", Integer.toString(soapEnvelope.length()));

      OutputStream reqStream = connection.getOutputStream();
      reqStream.write(soapEnvelope.getBytes());

      return connection.getInputStream();

    } catch (IOException e) {
      logger.error("Error during soap request to relatics: " + e.getMessage(), e);
      return null;
    }
  }
}
