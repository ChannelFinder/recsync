RecSync
=======

The record synchronizer project includes two parts.
A client (RecCaster) which runing as part of an EPICS
IOC, and a server (RecCeiver) which is a stand alone
daemon.  Together they work to ensure the the server(s)
have a complete list of all records currently provided
by the client IOCs.

Information Uploaded
--------------------

The RecCaster client currently sends the following information
about its Process Database.

* The EPICS Base Version
* A white listed set of envronment variables
* The name and type of all records
* Any info() tags associated with these records

RecCaster Usage
---------------

The RecCaster source in the `client/` sub-directory
is build as an EPICS support module.
It provides `reccaster.dbd` and the `reccaster` library.
The client demo IOC in `client/demoApp/` provides
an example of how to build RecCaster into an IOC.

RecCaster requires only EPICS Base 3.15 and later 3.14 releases.

RecCeiver Usage
---------------

The RecCeiver server in the `server/` directory is a
Python script using the [Twisted][twisted] networking
library.  It requires Python 2.7 and Twisted >= 12.0.

[twisted]: http://twistedmatrix.com/

The server uses the Twisted plugin interface to
make client information available to one or more
plugins.  See `server/demo.conf` for an example
configuration.

Currently two plugins are provided: `show` which print client
information to screen/log, and `db` which writes into a SQL
database (currently only sqlite3 supported).
The SQL table schema used is defined in `server/recceiver.sqlite3`.

Theory of Operation
===================

RecCaster
---------

The RecCaster client is an epics support module
which automatically starts a single thread and
begins passively waiting for a RecCeiver server
to announce its presence.

Upon receiving an announcement the client will
connect and upload its list of records and related
information to the server.  It then remains
connected indefinately and responds to periodic
alive tests from the server.

If this connection is lost the client will resume
waiting for another announcement.

RecCeiver
---------

The RecCeiver server periodially (every 15 seconds) sends
a broadcast announcement of it existance.

When a client connects it will wait for a server
handshake message before beginning to upload records.
The server will delay sending this message if too
many clients are currently uploading.

UDP Protocol
------------

Server announcement are made to UDP port 5049.

When sent to a IPv4 address,
such messages will have a length >= 16 bytes.
Clients will process only the first 16 bytes
and ignore any remaining bytes.

<table border="1">
  <tr>
    <td>0</td>
    <td>1</td>
    <td>2</td>
    <td>3</td>
    <td>4</td>
    <td>5</td>
    <td>6</td>
    <td>7</td>
    <td>8</td>
    <td>9</td>
    <td>A</td>
    <td>B</td>
    <td>C</td>
    <td>D</td>
    <td>E</td>
    <td>F</td>
  </tr>
  <tr>
    <td colspan="2">ID</td>
    <td>0</td>
    <td>X</td>
    <td colspan="4">SERV ADDR</td>
    <td colspan="2">PORT</td>
    <td>X</td>
    <td>X</td>
    <td colspan="4">SERV KEY</td>
  </tr>
</table>

* All multibyte fields have big-endian ordering.
* Fields marked `0` must be zero and clients will (silently) reject
messages where these fields are not zero.
* Fields marked `X` should be ignored by clients.
* The `ID` field must have the value 0x5243 (ascii 'RC').
* The `SERV ADDR` and `PORT` fields is the IPv4 address and TCP port to which clients should connect.
* `SERV KEY` is an arbitrary number which the client will later on the TCP connection.


TCP Protocol
------------

The TCP data stream will consist of a sequence of messages
having an 8 byte header followed by a variable length body.
The header will have the form

<table border="1">
  <tr>
    <td>0</td>
    <td>1</td>
    <td>2</td>
    <td>3</td>
    <td>4</td>
    <td>5</td>
    <td>6</td>
    <td>7</td>
  </tr>
  <tr>
    <td colspan="2">ID</td>
    <td colspan="2">MSGID</td>
    <td colspan="4">LEN</td>
  </tr>
</table>

* All multibyte fields have big-endian ordering.
* The `ID` field must have the value 0x5243 (ascii 'RC').
* The `MSGID` field identifies the type of message to follow.
* The `LEN` field gives the length in bytes or by message body to follow the header.
* Messages from server to client will have `MSGID` >= 0x8000.
* Messages from client to server will have `MSGID` < 0x8000.
* Except where explicitly stated, endpoints must silently ignore messages with unknown `MSGID`
* Endpoints must be prepared to receive and ignore extra body bytes.

*Messages*

<table border="1">
 <tr>
  <th>MSGID</th>
  <th>Min. LEN</th>
  <th>Name</th>
  <th>0</th>
  <th>1</th>
  <th>2</th>
  <th>3</th>
  <th>4</th>
  <th>5</th>
  <th>6</th>
  <th>7</th>
 </tr>
 <tr>
  <td>0x8001</td>
  <td>1</td>
  <td>Server Greet</td>
  <td>0</td>
 </tr>
 <tr>
  <td>0x8002</td>
  <td>4</td>
  <td>Ping</td>
  <td colspan="4">NONCE</td>
 </tr>
 <tr>
  <td>0x0001</td>
  <td>8</td>
  <td>Client Greet</td>
  <td>0</td>
  <td>0</td>
  <td>X</td>
  <td>X</td>
  <td colspan="4">SERV KEY</td>
 </tr>
 <tr>
  <td>0x0002</td>
  <td>4</td>
  <td>Pong</td>
  <td colspan="4">NONCE</td>
 </tr>
 <tr>
  <td>0x0003</td>
  <td>9</td>
  <td>Add Record</td>
  <td colspan="4">RECID</td>
  <td>ATYPE</td>
  <td>RTLEN</td>
  <td colspan="2">RNLEN</td>
  <td>RDLEN</td>
  <td>RTYPE</td>
  <td>RNAME</td>
  <td>RDESC</td>
 </tr>
 <tr>
  <td>0x0004</td>
  <td>4</td>
  <td>Del Record</td>
  <td colspan="4">RECID</td>
 </tr>
 <tr>
  <td>0x0005</td>
  <td>4</td>
  <td>Upload Done</td>
  <td>0</td>
  <td>0</td>
  <td>0</td>
  <td>0</td>
 </tr>
 <tr>
  <td>0x0006</td>
  <td>9</td>
  <td>Add Info</td>
  <td colspan="4">RECID</td>
  <td>KEYLEN</td>
  <td>X</td>
  <td colspan="2">VALEN</td>
  <td>KEY</td>
  <td>VALUE</td>
 </tr>
</table>

* `NONCE` is an number choosen by the server for a Ping message.  It must be echoed back by the client in a Pong message.
* `SERV KEY` must be the number the client received in the announcement from which it found this server.
* `RECID` is an identifier choosen by the client to identify a single record instance.  Except in Add Info, must be >0.
* `ATYPE` is 0 to add a record entry or 1 to add a record alias.  When adding an alias the record type is omitted (`RTLEN`==0) and the `RECID` must have been added in a previous message with `ATYPE`==0.
* `RTLEN`, `RNLEN`, `RDLEN`, `KEYLEN`, and `VALEN` are lengths in bytes.
* `RTLEN`, `RDLEN` and `VALEN` are allowed to be zero.
* `RNLEN` and `KEYLEN` must be greater than zero.
* `RTYPE`, `RNAME`, `RDLEN`, `KEY`, and `VALUE` are variable length fields whos length is given by the corresponding `*LEN` field.
* `RTYPE`, `RNAME`, `RDLEN`, `KEY`, and `VALUE` should not include a 0 (ascii null) byte.
* The `RECID` field of the Add Info may be zero.  When zero the key/value pair is considered to be associated with the client as a whole, and not any individual record.

When a TCP connection is established the client must send the Client Greeting message
immediately, then wait to receive the Server Greeting message before sending any
further messages.

Once the client has recevieved the Server Greeting it may send begin sending Add Record, Add Info, and Del Record messages.
Once the initial upload has been completed, the client must send Upload Done.

After Upload Done is received the server will periodically send Ping messages.  The client must respond to each Ping with a Pong.
If a client does not respond promptly the server may close the connection.
