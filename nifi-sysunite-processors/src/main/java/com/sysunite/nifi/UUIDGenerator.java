package com.sysunite.nifi;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

@Tags({"uuid, generate, random, attribute"})
@CapabilityDescription("Generates UUID and sets it on a attribute")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
@DynamicProperty(name = "Attribute Name", value = "Index of splitted line", supportsExpressionLanguage = false, description = "")
public class UUIDGenerator extends AbstractProcessor {

  public static final Relationship ORIGINAL = new Relationship.Builder()
    .name("original")
    .description("Input for this processor will be transferred to this relationship.")
    .build();

  public static final PropertyDescriptor ATTRIBUTE_NAME = new PropertyDescriptor
    .Builder().name("Attribute name")
    .description("The name of the attribute to set the UUID on")
    .required(true)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build();

  private List<PropertyDescriptor> descriptors;
  private AtomicReference<Set<Relationship>> relationships;

  @Override
  protected void init(final ProcessorInitializationContext context) {

    final List<PropertyDescriptor> descriptors = new ArrayList<>();
    descriptors.add(ATTRIBUTE_NAME);
    this.descriptors = Collections.unmodifiableList(descriptors);

    final Set<Relationship> relationships = new HashSet<>();
    relationships.add(ORIGINAL);
    this.relationships = new AtomicReference<>(relationships);
  }

  @Override
  public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
    return descriptors;
  }

  @Override
  public Set<Relationship> getRelationships() {
    return this.relationships.get();
  }

  @Override
  public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
    FlowFile flowFile = session.get();
    
    if (flowFile == null) {
      return;
    }

    flowFile = session.putAttribute(flowFile, context.getProperty(ATTRIBUTE_NAME).getValue(), UUID.randomUUID().toString());
    session.transfer(flowFile, ORIGINAL);
  }
}