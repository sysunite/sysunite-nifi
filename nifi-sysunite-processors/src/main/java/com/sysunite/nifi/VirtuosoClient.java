package com.sysunite.nifi;

import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import virtuoso.jena.driver.ISQLChannel;
import virtuoso.jena.driver.Virtuoso;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

@Tags({"sparql, virtuoso"})
@CapabilityDescription("A querying processor for Virtuoso.")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
public class VirtuosoClient extends AbstractProcessor {

  public static final PropertyDescriptor ADDRESS = new PropertyDescriptor
    .Builder().name("address")
    .description("The Virtuoso server address 'ip:port'")
    .required(true)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build();

  public static final PropertyDescriptor USER = new PropertyDescriptor
    .Builder().name("user")
    .description("Virtuoso login user")
    .required(true)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build();

  public static final PropertyDescriptor PASSWORD = new PropertyDescriptor
    .Builder().name("password")
    .description("Virtuoso login password")
    .required(true)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build();

  public static final PropertyDescriptor SEPARATOR = new PropertyDescriptor
    .Builder().name("result separator")
    .description("Separator used to separate column values.")
    .required(true)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build();

  public static final PropertyDescriptor SELECT = new PropertyDescriptor
    .Builder().name("select")
    .description("Comma separated select vars.")
    .required(true)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build();

  public static final PropertyDescriptor QUERY = new PropertyDescriptor
    .Builder().name("query")
    .description("The SPARQL query.")
    .required(true)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build();


  public static final Relationship SUCCESS = new Relationship.Builder()
    .name("success")
    .description("Relationship to transfer data to.")
    .build();

  private List<PropertyDescriptor> descriptors;

  private AtomicReference<Set<Relationship>> relationships;

  private volatile Set<String> dynamicPropertyNames;
  private Map<Relationship, PropertyValue> dynamicProperties;

  private Virtuoso quadStore;

  // url -> prefix (yeah, that's reversed logic)
  private Map<String, String> prefixMap;

  private ProcessorLog logger;

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
  public void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) {
    if(descriptor.isDynamic()) {

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

      // Build new relationships
      final Set<Relationship> newRelationships = new HashSet<>();


      newRelationships.add(SUCCESS);

      this.relationships.set(newRelationships);
    }
  }

  @OnScheduled
  public void onScheduled(final ProcessContext context) {
    for (final PropertyDescriptor descriptor : context.getProperties().keySet()) {
      if (descriptor.isDynamic()) {
        dynamicProperties.put(new Relationship.Builder().name(descriptor.getName()).build(), context.getProperty(descriptor));
      }
    }

    String address = context.getProperty(ADDRESS).getValue();
    String username = context.getProperty(USER).getValue();
    String password = context.getProperty(PASSWORD).getValue();

    quadStore = new Virtuoso("jdbc:virtuoso://"+address, username, password);
  }


  @Override
  protected void init(final ProcessorInitializationContext context) {

    logger = context.getLogger();

    final List<PropertyDescriptor> descriptors = new ArrayList<>();
    descriptors.add(ADDRESS);
    descriptors.add(USER);
    descriptors.add(PASSWORD);
    descriptors.add(SEPARATOR);
    descriptors.add(SELECT);
    descriptors.add(QUERY);
    this.descriptors = Collections.unmodifiableList(descriptors);

    final Set<Relationship> relationships = new HashSet<>();
    relationships.add(SUCCESS);
    this.relationships = new AtomicReference<>(relationships);

    // For dynamic properties
    this.dynamicPropertyNames = new HashSet<>();
    this.dynamicProperties = new HashMap<>();
  }

  @Override
  public Set<Relationship> getRelationships() {
    return this.relationships.get();
  }

  @Override
  public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
    return descriptors;
  }


  @Override
  public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

    prefixMap = new HashMap<>();
    for (final Map.Entry<Relationship, PropertyValue> dynamicProperty : dynamicProperties.entrySet()) {
      final String prefix = dynamicProperty.getKey().toString();
      final String uri = dynamicProperty.getValue().toString();

      prefixMap.put(uri, prefix);
    }

    String select = context.getProperty(SELECT).getValue();
    ArrayList<String> selectVars = new ArrayList<>();
    Collections.addAll(selectVars, select.split(","));

    String query = "sparql\n";
    for(String uri : prefixMap.keySet()) {
      query += "PREFIX "+prefixMap.get(uri)+": <"+uri+">\n";
    }
    query += context.getProperty(QUERY).getValue();

    String separator = context.getProperty(SEPARATOR).getValue();

    Statement stmt = ISQLChannel.executeQuery(quadStore.getVirtGraph(), query);
    try {
      ResultSet result = stmt.executeQuery(query);

      while(result.next()) {
        String flowFileRow = null;
        for(String selectVar : selectVars) {
          String value = result.getString(selectVar);
          for(String uri : prefixMap.keySet()) {
            if(value.startsWith(uri)) {
              value = prefixMap.get(uri) + ":" + value.substring(uri.length());
            }
          }
          if(value != null) {

            // Begin condition
            if(flowFileRow == null) {
              flowFileRow = value;
            }
            // For each column after the first
            else {
              flowFileRow += separator + value;
            }
          } else {
            logger.error("Wrong select!");
          }
        }

        FlowFile flowfile = session.create();
        flowfile = session.importFrom(new ByteArrayInputStream(flowFileRow.getBytes(StandardCharsets.UTF_8)), flowfile);
        session.transfer(flowfile, SUCCESS);

      }
    } catch (SQLException e) {
      logger.error(e.getMessage(), e);
    }
    finally {

      try {
        stmt.cancel();
        stmt.close();
      } catch (SQLException e) {
        logger.error(e.getMessage(), e);
      }
    }
  }


}
