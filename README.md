# global-identifiers

core.sql contains all the relevant tables and stored procedures to setup the Virtuoso Conductor. Simply execute the contents of the SQL file.

The Maven project DBpediaIndentifierClustering contains a java project for clustering and rewriting. See the comments in the main method of InMemoryClustering.java for further details.

## Data file rewriting 

Run the rewriting with 

<code>mvn exec:java -Dexec.mainClass="DataRewriter" -Dexec.args=""</code>

## Possible Dexec.args: 

**-con** *followed by the connection string to the database.*

**-in** *followed by the path to the folder containing the input data. Default is the data path*

**-out** *followed by the path to the folder for the data output. Default is the data/out path*

**-limit** *followed by the number of lines to rewrite per input file. Default is no line limit*

**-clustering** *followed by the name of the clustering to use in the rewriting process.*

**-buffer** *followed by the desired size of the local identifier cache. Default is no limit.*

**-namespace** *followed by a commalist of namespaces. Renaming will be restricted to the specified namespaces.*
