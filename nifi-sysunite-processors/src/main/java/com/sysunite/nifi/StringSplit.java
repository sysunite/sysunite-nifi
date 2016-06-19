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
    .description("Input for this processor will be transferred to this relationship.")
    .build();

  private List<PropertyDescriptor> properties;

  private volatile Set<String> dynamicPropertyNames = new HashSet<>();
  private Map<Relationship, PropertyValue> propertyMap = new HashMap<>();

  private AtomicReference<Set<Relationship>> relationships = new AtomicReference<>();


  @Override
  protected void init(final ProcessorInitializationContext context) {
    final List<PropertyDescriptor> properties = new ArrayList<>();
    properties.add(SPLIT);
    this.properties = Collections.unmodifiableList(properties);

    final Set<Relationship> set = new HashSet<>();
    set.add(ORIGINAL);
    this.relationships = new AtomicReference<>(set);
  }

  @Override
  protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {

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

    if(descriptor.isDynamic()) {

      final Set<String> newDynamicPropertyNames = new HashSet<>(dynamicPropertyNames);

      if (oldValue == null) {
        newDynamicPropertyNames.add(descriptor.getName());
      }

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

    if ( flowFile == null ) {
      return;
    }

    final AtomicReference<String> flowFileContentStore = new AtomicReference<>();

    session.read(flowFile, new InputStreamCallback() {

      @Override
      public void process(InputStream inputStream) throws IOException {

        try {
          flowFileContentStore.set(IOUtils.toString(inputStream));
        } catch (Exception e) {
          throw new ProcessException(e);
        }
      }});

    try {

      String flowFileContent = flowFileContentStore.get();
      String regexForSplit = context.getProperty(SPLIT).getValue();
      String[] splittedFlowFileContents = flowFileContent.split(regexForSplit);

      int totalSplits = splittedFlowFileContents.length;

      final Map<Relationship, PropertyValue> propMap = this.propertyMap;

      for (final Map.Entry<Relationship, PropertyValue> entry : propMap.entrySet()) {

        final PropertyValue value = entry.getValue();

        if(value.getValue() != null) {

          try {

            int partFromSplittedFlowFileContents = Integer.parseInt(value.getValue());

            if(partFromSplittedFlowFileContents < totalSplits){

              String splittedPart = splittedFlowFileContents[partFromSplittedFlowFileContents];
              String relationName = entry.getKey().getName();

              FlowFile fNew = session.clone(flowFile);
              fNew = session.putAttribute(fNew, relationName, splittedPart);
              session.transfer(fNew, entry.getKey());
            }

          } catch(Exception e) {
            throw new ProcessException(e);
          }
        }
      }

    } catch(Exception e){
      throw new ProcessException(e);
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
