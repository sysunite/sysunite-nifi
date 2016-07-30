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
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.ConfigurableComponent;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

@Tags({"xmlsplit, custom"})
@CapabilityDescription("Splits one or more nodes from a xml-file to a custom defined relationship")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
@DynamicProperty(name = "Relationship Name", value = "Xpath node name", supportsExpressionLanguage = false, description = "")
public class XmlSplit extends AbstractProcessor implements ConfigurableComponent {

  public static final Relationship ORIGINAL = new Relationship.Builder()
      .name("original")
      .description("Input for this processor will be transfered to this relationship.")
      .build();

  private List<PropertyDescriptor> properties;
  private volatile Set<String> dynamicPropertyNames = new HashSet<>();
  private Map<Relationship, PropertyValue> propertyMap = new HashMap<>();

  private AtomicReference<Set<Relationship>> relationships = new AtomicReference<>();

  @Override
  protected void init(final ProcessorInitializationContext context) {

    final List<PropertyDescriptor> properties = new ArrayList<>();
    this.properties = Collections.unmodifiableList(properties);
    final Set<Relationship> set = new HashSet<>();
    set.add(ORIGINAL);
    relationships = new AtomicReference<>(set);
  }


  @Override
  protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
    //position 1
    return new PropertyDescriptor.Builder()
        .required(false)
        .name(propertyDescriptorName)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .dynamic(true)
        .expressionLanguageSupported(false)
        .build();
  }


  @Override
  public void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue){



    if(descriptor.isDynamic()){

      final Set<String> newDynamicPropertyNames = new HashSet<>(dynamicPropertyNames);

      if (oldValue == null) {
        newDynamicPropertyNames.add(descriptor.getName());
      }

      if(newValue == null) {
        newDynamicPropertyNames.remove(descriptor.getName());
      }

      //TODO: what are we going to do with changed values from dynamic attributes?

      this.dynamicPropertyNames = Collections.unmodifiableSet(newDynamicPropertyNames);

      final Set<String> allDynamicProps = this.dynamicPropertyNames;

      final Set<Relationship> newRelationships = new HashSet<>();

      for (final String propName : allDynamicProps) {
        newRelationships.add(new Relationship.Builder().name(propName).build());
      }

      newRelationships.add(ORIGINAL);

      this.relationships.set(newRelationships);
    }
  }

  /* method required for dynamic property */
  @OnScheduled
  public void onScheduled(final ProcessContext context) {

    final Map<Relationship, PropertyValue> newPropertyMap = new HashMap<>();
    for (final PropertyDescriptor descriptor : context.getProperties().keySet()) {
      if (!descriptor.isDynamic()) {
        continue;
      }
      newPropertyMap.put(new Relationship.Builder().name(descriptor.getName()).build(), context.getProperty(descriptor));
    }

    this.propertyMap = newPropertyMap;
  }

  @Override
  public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

    FlowFile flowFile = session.get();

    if (flowFile == null) {
      return;
    }

    final AtomicReference<Document> theXml = new AtomicReference<>();

    session.read(flowFile, new InputStreamCallback() {

      @Override
      public void process(InputStream inputStream) throws IOException {

        try {
          DocumentBuilderFactory builderFactory =
              DocumentBuilderFactory.newInstance();
          DocumentBuilder builder = null;
          try {
            builder = builderFactory.newDocumentBuilder();
          } catch (ParserConfigurationException e) {
            System.out.println("invalid xml file content");
            e.printStackTrace();
          }

          Document xmlDocument = builder.parse(inputStream);

          theXml.set(xmlDocument);


        } catch(SAXException e) {
          System.out.println("invalid xml file content");
          e.printStackTrace();
        }
      }
    });




    // loop through the relations and get each value (xpath)
    final Map<Relationship, PropertyValue> propMap = propertyMap;

    for (final Map.Entry<Relationship, PropertyValue> entry : propMap.entrySet()) {

      final PropertyValue pv = entry.getValue();
      String xPathQuery = pv.getValue();

      final Relationship rel = entry.getKey();
      String relName = rel.getName();

      if (xPathQuery != null) {


        XPath xPath =  XPathFactory.newInstance().newXPath();

        // if we want an attribute of a node
        // we reconize the monkeytail in xpath i.e. /Node/@id - Route On Attribute (ori FileContent not changed)
        if(xPathQuery.matches("(.*)\u0040(.*)")) {

          String singleStringValue;
          try {
            singleStringValue = xPath.compile(xPathQuery).evaluate(theXml.get());
          } catch (XPathExpressionException e) {
            throw new ProcessException();
          }

          FlowFile fNew = session.clone(flowFile);
          fNew = session.putAttribute(fNew, relName, singleStringValue);
          session.transfer(fNew, rel);

        } else {

          NodeList nodeList;
          try {
            nodeList = (NodeList)xPath.compile(xPathQuery).evaluate(theXml.get(), XPathConstants.NODESET);
          } catch (XPathExpressionException e) {
            throw new ProcessException(e);
          }

          //extract all nodes and transfer them to the appropriate relation - Route On Content (ori FileContent changed)
          for (int i = 0; i < nodeList.getLength(); i++) {
            Node node = nodeList.item(i);
            StringWriter writer = new StringWriter();
            Transformer transformer = null;
            try {
              transformer = TransformerFactory.newInstance().newTransformer();
              transformer.transform(new DOMSource(node), new StreamResult(writer));

            } catch (TransformerException e) {
              throw new ProcessException(e);
            }


            String xml = writer.toString();

            FlowFile fNew = session.clone(flowFile);
            InputStream in = new ByteArrayInputStream(xml.getBytes());
            fNew = session.importFrom(in, fNew);
            session.transfer(fNew, rel);
          }
        }
      }
    }




    session.transfer(flowFile, ORIGINAL);
  }

  @Override
  public Set<Relationship> getRelationships() {
    return relationships.get();
  }

  @Override
  public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
    return properties;
  }
}