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

## Running youvegotdata.py
This must be run in a Python environment that includes `pika` - for connecting
to RabbitMQ -  and other needed packages. The `environ-3.8.yml` file in this
repository can be used to create a workable conda environment. Setting one up
using `pip` will certainly also work. Python 3.8 is the minimum version needed
to run the script. Higher versions should work.

Copy the template-config.ini file to config.ini and edit the config.ini as
described inside that file.
Run the code with:
```
python youvegotdata.py [-h] [-v] [-p PRODUCT] [-r VERSION] [-s START_TIME] [-e END_TIME] [-l LENGTH] [-c CHECKSUM] [-t CHECKSUM_TYPE] filepath
```
Run this with the -h (--help) argument to see the available flagged arguments.

This will usually be run with just the `filepath` argument. An example is:
```
python youvegotdata/youvegotdata.py /full/path/to/local/file/data_file.hdf
```
If run from a local repository of this project.

The `filepath` file must exist on the local machine.
