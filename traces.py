from pm4py.objects.ocel.importer.jsonocel import importer as jsonocel_importer
from pm4py.objects.log.exporter.xes import exporter as xes_exporter
from pm4py.objects.ocel.importer.jsonocel.importer import Variants
from pm4py.objects.log.obj import EventLog, Trace, Event
import networkx as nx
import pandas as pd
import pm4py
import math

ocel_file = "running-example.jsonocel"

# First method with extended table

# Upload jsonocel file and get extended table
ocel = jsonocel_importer.apply(ocel_file, variant=Variants.CLASSIC)
extended_table = ocel.get_extended_table() 

# Exporting traces per object type and saving them to xes files
object_types = [column for column in extended_table.columns if column.startswith("ocel:type:")]
for object_type in object_types:
    name_of_object_type = object_type.split(":")[-1]            
    # Get events with current object_type
    events = extended_table[extended_table[object_type].apply(lambda lst: isinstance(lst, list) and len(lst) > 0)]

    # If there are no events for this object type, skip to the next one
    if events.empty:
        continue
    # Explode the object_type column to create a row for each object
    events = events.explode(object_type)
    # Rename columns to match the event log format
    events = events.rename(columns={
        object_type: "case:concept:name",      
        "ocel:activity": "concept:name",
        "ocel:timestamp": "time:timestamp"})
    # Convert to event log
    event_log = pm4py.convert.convert_to_event_log(events) 

    # If the event log is empty, skip to the next object type
    if not event_log:
        continue
    for trace in event_log:
        for event in trace:
        # Remove NaN values from the event attributes
            for key, val in list(event.items()):
                if isinstance(val, float) and math.isnan(val):
                    del event[key]
    # Save the event log to a XES file
    pm4py.write_xes(event_log, f"1_traces_of_object_type_{name_of_object_type}.xes")

# Upload jsonocel file
ocel = pm4py.read_ocel(ocel_file)

# Second method with ocel objects and relations

# Get ocel objects
object_types = ocel.objects['ocel:type'].unique()

# Exporting traces per object type and saving them to xes files
for object_type in object_types:
    objects = ocel.objects[ocel.objects['ocel:type'] == object_type]['ocel:oid']# Get objects of the current type
    event_log = EventLog()# Initialize an empty event log

    # For each object, create a trace with its events
    for object_ID in objects:
        event_ID = ocel.relations[ocel.relations['ocel:oid'] == object_ID]['ocel:eid']# Get events related to the object
        events = ocel.events[ocel.events['ocel:eid'].isin(event_ID)].sort_values('ocel:timestamp')# Sort events by timestamp
        trace = Trace() # Initialize an empty trace for the object

        # Create an event dictionary with the required attributes
        for _, event in events.iterrows():
            event_dict = {
                "concept:name": event['ocel:activity'],
                "time:timestamp": event['ocel:timestamp'],
                "ocel:eid": event['ocel:eid']
            }

            # Add additional attributes from the event
            for col in event.index:
                if col not in ["ocel:activity", "ocel:timestamp", "ocel:eid"]:
                    val = event[col]
                    if pd.notna(val):
                        event_dict[col] = val
            # Create an Event object and add it to the trace
            trace.append(Event(event_dict))

        # Set the trace name to the object ID
        trace.attributes["concept:name"] = str(object_ID)
        event_log.append(trace)

    # Export the event log to a XES file
    xes_exporter.apply(event_log, f"2_traces_of_object_type_{object_type}.xes")

# Third method with connected components using a graph

# Create bipartite graph
graph = nx.Graph()
graph.add_nodes_from(ocel.events['ocel:eid'], bipartite=0) # Add events as one set of nodes
graph.add_nodes_from(ocel.objects['ocel:oid'], bipartite=1)# Add objects as another set of nodes
graph.add_edges_from([(row['ocel:eid'], row['ocel:oid']) for _, row in ocel.relations.iterrows()])# Add edges between events and objects

# Find connected components in the bipartite graph
components = list(nx.connected_components(graph))
# Get the events for each connected component 
component_events = []
for component in components:
    nodes = [n for n in component if n in set(ocel.events['ocel:eid'])] # Extract event nodes from the component
    events = ocel.events[ocel.events['ocel:eid'].isin(nodes)].sort_values('ocel:timestamp')# Sort events by timestamp
    component_events.append(events)
# Create an event log to store traces for each connected component    
event_log = EventLog()

# Create traces for each connected component
for i, events in enumerate(component_events):
    trace = Trace() # Initialize an empty trace for the component
    # For each event in the component, create an Event object and add it to the trace
    for _, event in events.iterrows():
        trace.append(Event({
            "concept:name": event['ocel:activity'],
            "time:timestamp": event['ocel:timestamp'],
            "eid": event['ocel:eid']
        }))
    # Set the trace name to the component number
    trace.attributes["concept:name"] = f"component_{i+1}"
    event_log.append(trace)# Add the trace to the event log

# Export the event log to a XES file
xes_exporter.apply(event_log, "3_connected_components_traces.xes")

# Upload jsonocel file
ocel = pm4py.read_ocel(ocel_file)

# Fourth method with traces by day using pandas datetime

# Get events and convert timestamp to date
events = ocel.events.copy()
events['date'] = pd.to_datetime(events['ocel:timestamp']).dt.date

# Group events by date and create traces
event_log = EventLog()# Initialize an empty event log
for date, group in events.groupby('date'):
    group = group.sort_values('ocel:timestamp') # Sort events by timestamp 
    trace = Trace() # Initialize an empty trace 
    # For each event in the group, create an Event object and add it to the trace
    for _, event in group.iterrows():
        trace.append(Event({
            "concept:name": event['ocel:activity'],
            "time:timestamp": event['ocel:timestamp'],
            "eid": event['ocel:eid'],
        }))
    # Set the trace name to the date
    trace.attributes["concept:name"] = str(date)
    event_log.append(trace)
    
# Export the event log to a XES file
xes_exporter.apply(event_log, "4_traces_by_day.xes")







