package com.sysunite.nifi;

import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.*;
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

  public static final Relationship SUCCESS = new Relationship.Builder()
    .name("success")
    .description("Relationship to transfer data to.")
    .build();

  private List<PropertyDescriptor> descriptors;

  private Set<Relationship> relationships;

  private ProcessorLog logger;

  @Override
  protected void init(final ProcessorInitializationContext context) {

    final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
    descriptors.add(URL);
    descriptors.add(REPORTNAME);
    descriptors.add(WORKSPACE);
    descriptors.add(ENTRYCODE);
    this.descriptors = Collections.unmodifiableList(descriptors);

    final Set<Relationship> relationships = new HashSet<Relationship>();
    relationships.add(SUCCESS);
    this.relationships = Collections.unmodifiableSet(relationships);

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


  @Override
  public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

    FlowFile flowfile = session.create();

    InputStream data = fetch(
      getSoapEnvelope(
        context.getProperty(REPORTNAME).getValue(),
        context.getProperty(WORKSPACE).getValue(),
        context.getProperty(ENTRYCODE).getValue()
      ),
      context.getProperty(URL).getValue()
    );

    flowfile = session.importFrom(data, flowfile);
    session.transfer(flowfile, SUCCESS);
  }

  public String getSoapEnvelope(String reportName, String workspace, String entryCode){

    return
      "<s:Envelope xmlns:s=\"http://www.w3.org/2003/05/soap-envelope\">\n" +
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
  }


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
