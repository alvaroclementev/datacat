# datacat

A timestamped data generator that you can use to replay and simulate 
production workloads.

This projects takes data from some source, performs a some small transformation 
(e.g. changing timestamps) and sends it to some "sink" at a given rate.
With this tool you can "replay" data from any supported source, sending it to 
a different location with updated time information for processing.

`datacat` makes it easy to configure the different steps in this pipeline:

- `source`: where the original data is read from
- `timestamper`: how to set or update timestamp field
- `conductor`: control the rate at which the data is produced
- `serializer`: output format of the data
- `sink`: destination in which the data is sent to
