package com.sysunite.nifi;

import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.*;

@Tags({"example, string-io"})
@CapabilityDescription("Creates a string from input (string literal or xml) and output it as FlowFile.")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
public class CreateString extends AbstractProcessor {

    public static final PropertyDescriptor INPUT = new PropertyDescriptor
            .Builder().name("input")
            .description("This property will contain the data to process i.e. string literal or xml piece.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Relationship to send data to.")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
      final List<PropertyDescriptor> descriptors = new ArrayList<>();
      descriptors.add(INPUT);
      this.descriptors = Collections.unmodifiableList(descriptors);

      final Set<Relationship> relationships = new HashSet<>();
      relationships.add(SUCCESS);
      this.relationships = Collections.unmodifiableSet(relationships);
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
      String value = context.getProperty(INPUT).getValue();
      InputStream data = new ByteArrayInputStream(value.getBytes());
      flowfile = session.importFrom(data, flowfile);
      session.transfer(flowfile, SUCCESS);
    }
}
