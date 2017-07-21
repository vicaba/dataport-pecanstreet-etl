# Dataport Pecanstreet ETL

This programs extracts data from the dataport pecanstreet database.
This program is prepared to extract data in batches of a given time duration in the time interval from year 2012 to now.
It first guesses the time column and then executes queries for the given duration incrementally.

q0_range = \[t + 1, t + ∂\] 

q1_range = \[t + 1 + ∂, t + 2∂\]

As you can see in the previous formula, the start time is the given time (t) plus 1 in order to avoid time overlap.

## How to package this program

* Run `sbt`.
* Execute `universal:packageBin`
* A .zip with the executable can be found at `/target/universal/`

## How to upload this program to a server

Use the `scp` command. For example: `scp /some/local/directory your_username@remotehost.edu:foobar.txt`

## How to configure and run this program

To provide a configuration file to the Java executable use the `-Dconfig.file=` flag.
For example: `sudo dataport-pecanstreet-etl-1.0/bin/dataport-pecanstreet-etl -Dconfig.file="/home/vagrant/app.conf" & `

