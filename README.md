# youvegotdata

Uses RabbitMQ to send new file notifications, with the ultimate purpose of
getting the file metadata into the Data Inventory Database.

The "producer" `youvegotdata.py` will usually be called by the CIRA data
ingest scripts when a new file is added to the CIRA data stores, and will send
a message through RabbitMQ to the consumers with the file's metadata.

Message "consumers" will be running to receive the file metadata and insert it
into the database. It is expected that multiple consumers process will be
accepting messages in RabbitMQ's "fair dispatch" configuration. A given
notification will be received by one consumer.

## Installing youvegotdata
This module requires Python 3.8 or greater.
It is deployed to PyPi, so it can be installed with:
```
pip install youvegotdata
```
Which will install the `ygd` CLI command in your current Python environment.

## Running youvegotdata.py as ygd
Create the ~/.config/youvegotdata/ directory if it does not already exist.
Create a `config.ini` file in this directory that looks like:
```
[Settings]
RMQ_HOST = <host of the RabbitMQ server>
```
And fill it in with the RabbitMQ server's host name.

Run the code with:
```
ygd [-h] [-v] [-p PRODUCT] [-r VERSION] [-s START_TIME] [-e END_TIME] [-l LENGTH] [-c CHECKSUM] [-t CHECKSUM_TYPE] filepath
```
Run this with the -h (--help) argument to see the available flagged arguments.

This will usually be run with just the `filepath` argument. An example is:
```
ygd /full/path/to/local/file/data_file.hdf
```
If run from a local repository of this project.

The `filepath` file must exist on the local machine.
