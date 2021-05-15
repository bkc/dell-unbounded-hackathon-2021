# SGF - Speedy Goods Freight

dell-unbounded-hackathon-2021

SGF is a parcel delivery service that prides itself on fast delivery with a money-back guarantee for late or lost packages. To meet SGF business goals they rely on an in-house package tracking system implemented by this project.

## Package processing work-flow

SGF assigns a unique identifier to every package, along with meta-data such as package weight, destination and declared value. This information is encoded into a barcode placed on the package.

Packages are initially received at any one of four sorting centers where they are first weighed before being routed to their destination.

Within each sorting center packages travel on a series of conveyor belts. Barcode scanners are placed at branch points to detect and route packages as they travel within the sorting center. Each package may be scanned by four or more barcode scanners as they pass through the sorting center.

Packages may be routed to other sorting centers for final delivery, or they may be delivered directly from the local sorting center.

Packages to be routed to a remote sorting center are sent to a holding area for that sorting center where they accumulate before being placed on a truck. Trucks travel between sorting centers, once per hour.

## Package tracking system capabilities

The package tracking system must provide the following functionality:

* Detect when packages have fallen off a conveyor belt within a sorting center, so that they can be located and placed back into the package flow. 
* Detect when packages are lost
* Calculate the count, total weight and total value of packages loaded on each truck
* Calculate the running average of package count and value per sorting center to determine liability insurance requirements and required staffing levels
* Calculate the running average of lost or delayed packages by sorting center and by conveyor belt section
* Provide a public facing package tracking interface that shows top-level package events for individual packages (entry and exit of sorting centers, delivery date and time)
* Provide an internal facing package tracking interface that shows all package events for individual packages

## Non-functional requirements

* Must use Pravega (good speed) to accumulate barcode scans
* Sorting Center processing should independent from other sorting centers
* The internal package tracking interface can also be per-sorting center
* The public facing tracking interface must collect scan events from all sorting centers. In a real production situation we would likely use something like Nats Jetstream for inter-sorting center communication. However for the purposes of this simulation we will use Pravega for both data accumulation and "communication" between sorting centers

## Hackathon simulation limitations

Barcode scanning events will be generated by a simulation program.

To simplify the development tasks for the hackathon we make the following assumptions:

* Packages are considered to be received when they reach the intake scanner at the first sorting center they reach and delivered when they reach the output holding area of the final sorting center. We do not track last mile pickup or delivery in this simulation.
* Any given package will travel through at most two sorting centers
* There will be four sorting centers
* Each sorting center has the following barcode scanners:
  * pre-routing
  * routing
  * holding A, B, C (when routing to remote centers)
  * loaded A,B, C (a virtual scan when a package is loaded into a truck) corresponds to the truck departure time
  * output (local delivery)
* Packages enter a sorting center in one of two ways:
  * when receiving packages from customers: intake scan followed by a weighing scan 
  * when receiving packages from other sorting centers: receiving scan
* Typically packages take between 1 to 5 days to be delivered, but the simulation will only run for a few minutes
* The total number of simulated packages will vary depending on system performance
* Packages are not scanned on trucks. Instead the loading scan and the receiving scan will be used to determine when a package is on a truck
* There are unlimited number of trucks that depart for remote sorting centers once per hour and always arrive 'on time'. The time to travel between individual sorting centers is fixed, but depends on origin and destination. e.g. from center A to C - 48 hours, from A to B - 24 hours, etc.
* The simulator will fabricate a specified number of lost packages, most of which will be found 'late' and some of which will never be found. This will generate lost package events in sorting centers and lost/late deliveries that must be refunded to the customer

# Solution proposal

Brainstorm how to use Pravega to provide package tracking capabilities

## Detect when packages have fallen off a conveyor belt within a sorting center, so that they can be located and placed back into the package flow

When packages are scanned, an event is sent to the sorting center master stream with these elements:

* timestamp
* package id
* barcode scanner id
* next destination barcode scanner id
* expected next barcode scan time

A per-sorting center processing task will:

* push the package id and its next barcode scan time into a redis 'late' sorted set (after removing any already existing instance of the package id)
* if the barcode scanner id is an intake, receiving, output or loaded scan then also send a copy of the event to the central public tracking event stream
* if the barcode scanner id is an intake scanner, it will calculate the expected delivery time and push the package id and eta to a redis 'delivery' sorted set  to track late deliveries
* if the barcode scanner id is an output scanner, it will remove the package id from the redis 'delivery' sorted set

Another thread or process will monitor the redis 'late' sorted set to look for packages whose expected next barcode scan is 'in the past' and then report them as missing. These packages could be found and put onto a conveyor belt

## Detect when packages are lost

The redis 'delivery' sorted set will indicate which packages were not delivered in time

## Calculate the count, total weight and total value of packages loaded on each truck

When packages arrive in a holding area, they will be added to a per-truck stream. At the 'next hour' mark the stream will be closed. The count, weight and value will be calculated from all packages in that truck's stream


TODO

* Calculate the running average of package count and value per sorting center to determine liability insurance requirements and required staffing levels
* Calculate the running average of lost or delayed packages by sorting center and by conveyor belt section
* Provide a public facing package tracking interface that shows top-level package events for individual packages (entry and exit of sorting centers, delivery date and time)
* Provide an internal facing package tracking interface that shows all package events for individual packages


# Operation
How to use this software to demonstrate Pravega functionality

The first step is to generate a stream of simulated package tracking events to be written to Pravega streams.

## Simulator CLI

The simulator Cli is used to generate a stream of json events on stdout. These events  must be sorted on key `event_time` before ingesting into Pravega

```shell
usage: simulator_cli.py [-h] [-s SIMULATED_RUN_TIME] [-i INTAKE_RUN_TIME]
                        [-p PACKAGE_COUNT] [-d DELAYED_PACKAGE_COUNT]
                        [--lost_package_count LOST_PACKAGE_COUNT] [-t] [-j]
                        [-l {info,warn,debug,error,fatal,critical}]

optional arguments:
  -h, --help            show this help message and exit
  -s SIMULATED_RUN_TIME, --simulated_run_time SIMULATED_RUN_TIME
                        total simulated running time (minutes, real-world
                        time, e.g. 1440 = 1 day)
  -i INTAKE_RUN_TIME, --intake_run_time INTAKE_RUN_TIME
                        total simulated running time to intake packages
                        (minutes)
  -p PACKAGE_COUNT, --package_count PACKAGE_COUNT
                        total number of packages to be simulated
  -d DELAYED_PACKAGE_COUNT, --delayed_package_count DELAYED_PACKAGE_COUNT
                        total number of packages to be delayed
  --lost_package_count LOST_PACKAGE_COUNT
                        total number of packages to be lost enroute (must be
                        less than delayed_package_count)
  -t, --test            run simulation test
  -j, --json_output     output json
  -l {info,warn,debug,error,fatal,critical}, --console_log_level {info,warn,debug,error,fatal,critical}
                        set logging level for console output:
                        info,warn,debug,error,fatal,critical
```



Example: intake 1000 packages in 8 simulated hours, then continue generating events for 6 days, delaying 20 packages. Of those 20 packages, 5 will be completely lost, write the sorted json stream to a file for later ingestion into Pravega



```shell
$ python simulator_cli.py -t --package_count 1000 --intake_run_time 480 --simulated_run_time 8640 --delay 20 --lost 5 -j | jq -sc 'sort_by(.event_time)[]'  > /tmp/events.json
```

The jq tool is used to sort and ensure the generated output is one complete json object per text line

```json
{"estimated_delivery_time":1621138133,"event_time":1621046333,"next_scanner_id":"weighing","scanner_id":"intake","next_event_time":1621046573,"destination":"C","declared_value":38,"package_id":"1","sorting_center":"B"}
{"estimated_delivery_time":1621138161.8,"event_time":1621046361.8,"next_scanner_id":"weighing","scanner_id":"intake","next_event_time":1621046601.8,"destination":"A","declared_value":73,"package_id":"2","sorting_center":"B"}
{"estimated_delivery_time":1621483790.6,"event_time":1621046390.6,"next_scanner_id":"weighing","scanner_id":"intake","next_event_time":1621046630.6,"destination":"C","declared_value":46,"package_id":"3","sorting_center":"D"}
{"estimated_delivery_time":1621138219.3999999,"event_time":1621046419.3999999,"next_scanner_id":"weighing","scanner_id":"intake","next_event_time":1621046659.3999999,"destination":"C","declared_value":32,"package_id":"4","sorting_center":"B"}
```


