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

import com.jcabi.xml.XML;
import com.jcabi.xml.XMLDocument;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.components.ConfigurableComponent;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
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

  //user-defined properties i.e. MY_PROPERTY
  private List<PropertyDescriptor> properties;

  //seperate lists for dynamic properties
  private volatile Set<String> dynamicPropertyNames = new HashSet<>();
  private Map<Relationship, PropertyValue> propertyMap = new HashMap<>();

  private AtomicReference<Set<Relationship>> relationships = new AtomicReference<>();

  @Override
  protected void init(final ProcessorInitializationContext context) {
    //position 0
    final List<PropertyDescriptor> properties = new ArrayList<>();
    //properties.add(SPLIT);
    this.properties = Collections.unmodifiableList(properties);

    final Set<Relationship> set = new HashSet<>();
    set.add(ORIGINAL);
    relationships = new AtomicReference<>(set);
  }

  /* method required for dynamic property */
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

  /* method from interface ConfigurableComponent */
  /*method required for dynamic property */
  @Override
  public void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue){

    //position 2

    if(descriptor.isDynamic()){


      //------------first we make dynamic properties
      //System.out.println("@onPropertyModified:: dynamic prop");

      final Set<String> newDynamicPropertyNames = new HashSet<>(dynamicPropertyNames);

      if (oldValue == null) {    // new property
        //System.out.println("@onPropertyModified::oldValue=NULL");
        //newDynamicPropertyNames.addAll(this.dynamicPropertyNames);
        newDynamicPropertyNames.add(descriptor.getName());
        //dynamicPropertyValues.put(descriptor, newValue);
      }

      if(newValue == null){  //property removal!
        newDynamicPropertyNames.remove(descriptor.getName());
        //System.out.println("property removed!");
      }

      //TODO: what are we going to do with changed values from dynamic attributes?

      this.dynamicPropertyNames = Collections.unmodifiableSet(newDynamicPropertyNames);


      //------------then we make relationships with the dynamic property names
      final Set<String> allDynamicProps = this.dynamicPropertyNames;

      final Set<Relationship> newRelationships = new HashSet<>();

      for (final String propName : allDynamicProps) {
        newRelationships.add(new Relationship.Builder().name(propName).build());
      }

      newRelationships.add(ORIGINAL); //dont forget the original

      this.relationships.set(newRelationships);

    }

  }

  /* method required for dynamic property */
  @OnScheduled
  public void onScheduled(final ProcessContext context) {
    //position 3

    //System.out.println("@onScheduled");

    final Map<Relationship, PropertyValue> newPropertyMap = new HashMap<>();
    for (final PropertyDescriptor descriptor : context.getProperties().keySet()) {
      if (!descriptor.isDynamic()) {
        continue;
      }
      //getLogger().debug("Adding new dynamic property: {}", new Object[]{descriptor});
      //System.out.println("Adding new dynamic property: {}" + descriptor.toString());
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


    //read the flow file and save it contents
    final AtomicReference<InputStream> theXml = new AtomicReference<>();

    session.read(flowFile, new InputStreamCallback() {

      @Override
      public void process(InputStream isIn) throws IOException {

        //System.out.println("contact!");

        try {

          String contents = IOUtils.toString(isIn);

          XML xmlNode = new XMLDocument(contents);

          InputStream is = new ByteArrayInputStream(xmlNode.toString().getBytes());

          theXml.set(is);

        } catch(IOException e){
          System.out.println("w00t");// + e.getMessage());
        }catch(IllegalArgumentException e){
          System.out.println("is xml niet geldig?");
        }catch(IndexOutOfBoundsException e){
          System.out.println("bah! de node waar gezocht naar moet worden is niet gevonden!");
        }

      }
    });

    //fetch the xml content again

    try {

      InputStream orig_xml = theXml.get();

      String xml_contents = IOUtils.toString(orig_xml);

      try {

        //loop through the relations and get each value (xpath)
        final Map<Relationship, PropertyValue> propMap = propertyMap;

        for (final Map.Entry<Relationship, PropertyValue> entry : propMap.entrySet()) {

          final PropertyValue pv = entry.getValue();
          String xpath = pv.getValue();

          final Relationship rel = entry.getKey();
          String relName = rel.getName();

          if (xpath != null) {

            System.out.println(xpath);

            //create new xml
            XML file = new XMLDocument(xml_contents);

            //if we want an attribute of a node
            //we reconize the monkeytail in xpath i.e. /Node/@id - Route On Attribute (ori FileContent not changed)
            if(xpath.matches("(.*)\u0040(.*)")){
              String v = file.xpath(xpath).get(0);
              //System.out.println(v);

              FlowFile fNew = session.clone(flowFile);
              //create attribute
              fNew = session.putAttribute(fNew, relName, v);
              //transfer
              session.transfer(fNew, rel);

            }else {

              //extract all nodes and transfer them to the appropriate relation - Route On Content (ori FileContent changed)
              for (XML ibr : file.nodes(xpath)) {

//              System.out.println("match!");
//              System.out.println(ibr.toString());

                FlowFile fNew = session.clone(flowFile);
                //create attribute
                //fNew = session.putAttribute(fNew, relName, ibr.toString());

                InputStream in = new ByteArrayInputStream(ibr.toString().getBytes());

                fNew = session.importFrom(in, fNew);
                //transfer
                session.transfer(fNew, rel);

              }

            }

          }

        }

      } catch(IllegalArgumentException e){
        System.out.println("is xml niet geldig?");
      }catch(IndexOutOfBoundsException e){
        System.out.println("bah! de node waar gezocht naar moet worden is niet gevonden!");
      }

    }catch(IOException e){
      System.out.println("cannot read xml");
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
