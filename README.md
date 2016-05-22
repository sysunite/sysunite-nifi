# Apache-Nifi

Generic Sysunite processors for Apache NiFi.

## CreateString
This processor creates flowfile content of a component-property value.
Its input is a String given as value to static component-property called 'INPUT' and outputs the result to the succes-relation.

## RestClient
This processor is designed to create a REST call to an http-endpoint, based on a soap-envelope structure.
The fetched data will be new flowfile contents and output its result to the success-relation.

## StringSplit
This processor splits a String literal by a regex-match i.e. ';' and output the result to a custom relation.
To make this possible, the user is allowed to create dynamic properties, which define a unique relation for every part of the string-split result to send to.

I.e. if the input is "Hello;World", then you can add component properties as follows:
Where 'first_part' define the relation name, and '0' as value (which refers to part of the input that is split).
You can connect a new component with relation 'firt_part'. Input for this component will be 'Hello'.

## XmlSplit
This processor uses Jcabi-parser to read an XML-file and depending of the value of the dynamic attributes defined by the user, there are a few options.
1. If the value of the dynamic attribute has an intention to find an attribute of an xml-node, i.e. '/Node/@id' then that result of that match,
will be saved as value of a flowfile attribute, which is put on the original flowfile. The attribute can later be retrieved by the dynamic property name. Original flowfile contents will not change.
2. If the value of the dynamic attribute has an intention to find a node (or child node), i.e. '/Node/childNode', then that node is extracted
and the original flowfile contents will be replaced for this result.
In both cases, the result will be send to a relation that equals the dynamic property name it belongs too.

