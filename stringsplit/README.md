# Apache-Nifi

This processor splits an string literal i.e. "marie;leerde;jantje;lopen;langs;de;lange;lindelaan" and outputs its result to one or more dynamic defined relationships, with a splitted part as FlowFile-attribute. The name of each relationship equals a dynamic property. Its idea is based on the Apache NiFi RouteOnAttribute-component.
