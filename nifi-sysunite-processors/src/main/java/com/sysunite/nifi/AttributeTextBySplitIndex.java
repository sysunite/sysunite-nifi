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

@Tags({"routing, route, text, index"})
@CapabilityDescription("Puts textual data on attributes based on the splitted index. " +
  "Each line in an incoming FlowFile is splitted against a pattern and the indexes specified by user-defined Properties. " +
  "The data is then put on attributes according to these rules.")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
@DynamicProperty(name = "Attribute Name", value = "Index of splitted line", supportsExpressionLanguage = false, description = "")
public class AttributeTextBySplitIndex extends AbstractProcessor {

  public static final Relationship ORIGINAL = new Relationship.Builder()
    .name("original")
    .description("Input for this processor will be transferred to this relationship.")
    .build();
  
  public static final PropertyDescriptor SKIP_LINES = new PropertyDescriptor
    .Builder().name("Skip lines")
    .description("Number of lines to skip until the actual line to be splitted and routed by index.")
    .required(false)
    .addValidator(StandardValidators.INTEGER_VALIDATOR)
    .build();

  public static final PropertyDescriptor PATTERN = new PropertyDescriptor
    .Builder().name("Text pattern")
    .description("Recurring text pattern to split on")
    .required(true)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build();
  
  private List<PropertyDescriptor> descriptors;
  private AtomicReference<Set<Relationship>> relationships;

  private volatile Set<String> dynamicPropertyNames;
  private Map<String, PropertyValue> propertyMap;

  @Override
  protected void init(final ProcessorInitializationContext context) {

    final List<PropertyDescriptor> descriptors = new ArrayList<>();
    descriptors.add(SKIP_LINES);
    descriptors.add(PATTERN);
    this.descriptors = Collections.unmodifiableList(descriptors);

    final Set<Relationship> relationships = new HashSet<>();
    relationships.add(ORIGINAL);
    this.relationships = new AtomicReference<>(relationships);
    
    // For dynamic properties
    this.dynamicPropertyNames = new HashSet<>();
    this.propertyMap = new HashMap<>();
  }

  @Override
  protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
    return new PropertyDescriptor.Builder()
      .required(false)
      .name(propertyDescriptorName)
      .addValidator(StandardValidators.INTEGER_VALIDATOR)
      .dynamic(true)
      .expressionLanguageSupported(false)
      .build();
  }

  @Override
  public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
    return descriptors;
  }
  

  @Override
  public void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue){
    if(descriptor.isDynamic()){
      
      final Set<String> newDynamicPropertyNames = new HashSet<>(dynamicPropertyNames);

      // New property
      if (oldValue == null && newValue != null) {
 
        newDynamicPropertyNames.add(descriptor.getName());
      }
      // Remove property
      else if(newValue == null) {
        newDynamicPropertyNames.remove(descriptor.getName());
      }
      
      this.dynamicPropertyNames = Collections.unmodifiableSet(newDynamicPropertyNames);
    }
  }

  @OnScheduled
  public void onScheduled(final ProcessContext context) {     
    for (final PropertyDescriptor descriptor : context.getProperties().keySet()) {
      if (descriptor.isDynamic()) {
        propertyMap.put(descriptor.getName(), context.getProperty(descriptor));
      }
    }
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

    // Read contents
    final AtomicReference<String> contents = new AtomicReference<>();
    session.read(flowFile, inputStream -> contents.set(IOUtils.toString(inputStream)));
    String toBeSplit = contents.get();

    // Skip lines
    if(context.getProperty(SKIP_LINES).isSet()){
      int skipLines = Integer.valueOf(context.getProperty(SKIP_LINES).getValue());
      String[] lines = toBeSplit.split("\n");
      
      if (lines.length <= skipLines) {
        throw new ProcessException("Could not skip lines by " + skipLines + ", there are insufficient lines (" + lines.length + ")");
      }
        
      toBeSplit = lines[Integer.valueOf(skipLines)];
    }
    
    // Split
    String pattern = context.getProperty(PATTERN).getValue();
    String[] splitted = toBeSplit.split(pattern);

    for (final Map.Entry<String, PropertyValue> dynamicProperty : propertyMap.entrySet()) {
      final String name = dynamicProperty.getKey();

      final PropertyValue propertyValue = dynamicProperty.getValue();
      int index = Integer.valueOf(propertyValue.getValue());

      if(index >= splitted.length){
        getLogger().warn("Attribute " + name + " needs an index higher than the splitted line");
        flowFile = session.putAttribute(flowFile, name, ""); // TODO: should it allow for null?
      }
      else {
        // Set attribute
        flowFile = session.putAttribute(flowFile, name, splitted[index]);
      }
    }

    session.transfer(flowFile, ORIGINAL);
  }
}