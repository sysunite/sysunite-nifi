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

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

@Tags({"string, split, relations"})
@CapabilityDescription("Split a string and transfer parts to custom relations.")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
@DynamicProperty(name = "Relationship Name", value = "Attribute Expression Language", supportsExpressionLanguage = false, description = "blabla")
public class StringSplit extends AbstractProcessor implements ConfigurableComponent {

  public static final PropertyDescriptor SPLIT = new PropertyDescriptor
    .Builder().name("split")
    .description("Regex used to split the input i.e. ';' ")
    .required(true)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build();

  public static final Relationship ORIGINAL = new Relationship.Builder()
          .name("original")
          .description("Input for this processor will be tranfered to this relationship.")
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
    properties.add(SPLIT);
    this.properties = Collections.unmodifiableList(properties);

    final Set<Relationship> set = new HashSet<>();
    set.add(ORIGINAL);
    this.relationships = new AtomicReference<>(set);
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
    //position 4

    FlowFile flowFile = session.get();

    if ( flowFile == null ) {
      return;
    }

    int totalDynamicRelations = this.getRelationships().size();

    System.out.println("rels= " + totalDynamicRelations);

    //-------reading
    final AtomicReference<String> flowFileContentStore = new AtomicReference<>();

    session.read(flowFile, new InputStreamCallback() {

      @Override
      public void process(InputStream inputStream) throws IOException {

        System.out.println("read");

        try {

          String flowFileContent = IOUtils.toString(inputStream);
          flowFileContentStore.set(flowFileContent);

        } catch (Exception e) {

        }

      }});

    //-------using en editing
    try{

      //get contents
      String flowFileContent = flowFileContentStore.get();

      //split contents by delimiter
      String regexForSplit = context.getProperty(SPLIT).getValue();
      String[] splittedFlowFileContents = flowFileContent.split(regexForSplit);

      //count splits
      int totalSplits = splittedFlowFileContents.length;

      System.out.println("split= " +  totalSplits);

      //get list of dynamic props
      final Map<Relationship, PropertyValue> propMap = this.propertyMap;

      for (final Map.Entry<Relationship, PropertyValue> entry : propMap.entrySet()) {

        final PropertyValue value = entry.getValue();
        String oriValue = value.getValue();

        //use only splits not empty
        if(oriValue != null) {

          try {

            int partFromSplittedFlowFileContents = Integer.parseInt(oriValue);

            //parts only in range are allowed
            if(partFromSplittedFlowFileContents < totalSplits){

              String splittedPart = splittedFlowFileContents[partFromSplittedFlowFileContents];

              String relationName = entry.getKey().getName();

              System.out.println("relationship (" + relationName + ") -> " + splittedPart );

              //now we can tranfer splitted part to the relation

              FlowFile fNew = session.clone(flowFile);
              fNew = session.putAttribute(fNew, relationName, splittedPart);
              session.transfer(fNew, entry.getKey());

            }

          }catch(Exception e){

          }

        }

      }


    }catch(Exception e){

    }


    session.transfer(flowFile,ORIGINAL);


  }


  @Override
  public Set<Relationship> getRelationships () {
    return relationships.get();
  }

  @Override
  public final List<PropertyDescriptor> getSupportedPropertyDescriptors () {
    return properties;
  }


}
