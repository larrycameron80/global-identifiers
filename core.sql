drop table DBpediaSingletonMap;
drop table DBpediaLinkMap;
drop table DBpediaIdCounter;
drop table SourceMap;
drop table DBpediaClusteringMap;

-- Maps any uri to a bigint identifier
create table DBpediaSingletonMap
(
	"DBpediaId" VARCHAR PRIMARY KEY,
	"SingletonId" BIGINT
);

-- Contains information about link sources, currently just some mockup content
create table SourceMap
(
	"SourceId" BIGINT IDENTITY,
	"SourceName" VARCHAR,
	"SourcePrefix" VARCHAR
);

-- A global bigint counter to generate new singleton ids for the DBpediaSingletonMap
create table DBpediaIdCounter
(
	"Counter" BIGINT NOT NULL
);

-- Contains all the links that have been inserted to the database with a bigint identifier and the 
-- singleton ids of the uris connected by the link
create table DBpediaLinkMap
(
	"LinkId" BIGINT IDENTITY,
	"SingletonId1" BIGINT NOT NULL,
	"SingletonId2" BIGINT NOT NULL,
	"Relation" VARCHAR NOT NULL,
	"InsertDate" DATETIME
);

-- Will keep track of all the clustering that have been created so far
create table DBpediaClusteringMap
(
	"ClusteringName" VARCHAR PRIMARY KEY,
	"CreationDate" DATETIME,
	"LinkCount" BIGINT
);

commit work;
delay(1);


--used to normalize input uri, currently unused
create procedure DBpediaNormalizeIri
(
	in uri VARCHAR
)
{
	--TODO check if uri use rfc1808_parse_uri (url) function see: https://github.com/chile12/DataEventFramework/blob/master/VirtuosoSql%20%26%20Doku/CentralDB.sql#L238 ff
	DECLARE cleaned VARCHAR;

	cleaned := TRIM(LOWER(uri));

	--TODO some more normalization??
	return cleaned;
};


-- Creates a redirect between two clusterings. The source clustering that will pass its cluster ids to the 
-- target clustering. This procedure will also create a redirect map for the target clustering which will resolve
-- requests to deprecated cluster identifiers
create procedure DBPediaCreateRedirectMap
(
	in sourceClustering VARCHAR, 
	in targetClustering VARCHAR
)
{
	-- first: check, if source and target clustering exist in the database
	DECLARE sourceMap, targetMap, redirectMap VARCHAR;
	sourceMap := sprintf('%s_view', sourceClustering);
	targetMap := sprintf('%s_view', targetClustering);
	redirectMap := sprintf('%s_redirect', targetClustering);

	DECLARE sourceExists, targetExists, redirectExists ANY;
	sourceExists := (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = sourceMap);
	targetExists := (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = targetMap);

	IF ((LENGTH(sourceExists) = 0) OR (LENGTH(targetExists) = 0))
	{
		-- Return, if target or source clustering do NOT exist
		SIGNAL('INVALID', 'SOURCE OR TARGET CLUSTERING DOES NOT EXIST');
		RETURN;
	}

	-- check, if redirect map already exists
	DECLARE redirectExists ANY;
	redirectExists := (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = redirectMap);

	-- If there is no redirect yet, create a new one!
	if(LENGTH(redirectExists) = 0)
	{
		EXEC(sprintf('CREATE TABLE %s_redirect ("SourceClustering" VARCHAR, "SourceClusterId" BIGINT NOT NULL,
			"TargetClusterId" BIGINT NOT NULL)', targetClustering));	
	}

	SIGNAL('HELLO THERE!', 'READY TO CONTINUE...');
};

-- Creates a clustering for all links added between fromDate and toDate
create procedure DBPediaCreateClusterByDate
(
	in tableNamePrefix VARCHAR,
	in fromDate DATETIME,
	in toDate DATETIME
)
{

	DECLARE linkMap, viewMap VARCHAR;
	linkMap := sprintf('%s_links', tableNamePrefix);
	viewMap := sprintf('%s_view', tableNamePrefix);

	DECLARE tableExists ANY;
	tableExists := (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = linkMap);

	-- If there are no tables for the current clustering, create new tables!
	if(LENGTH(tableExists) = 0)
	{
		EXEC(sprintf('
			CREATE TABLE %s_links ("LinkId" BIGINT PRIMARY KEY, "ClusterId" BIGINT NOT NULL)
			CREATE TABLE %s_view ("SingletonId" BIGINT PRIMARY KEY, "ClusterId" BIGINT NOT NULL)
			CREATE TABLE %s_history ("LinkId" BIGINT NOT NULL, "Time" DATETIME, "Type" VARCHAR NOT NULL)', 
			tableNamePrefix, tableNamePrefix, tableNamePrefix, tableNamePrefix, tableNamePrefix, viewMap));	
	}

	-- Select from global link map to insert into link map and view map
	FOR SELECT LinkId, SingletonId1, SingletonId2 FROM DBpediaLinkMap WHERE InsertDate > fromDate 
		AND InsertDate < toDate DO {
		DBPediaAddLinkToCluster(LinkId, SingletonId1, SingletonId2, linkMap, viewMap);
	}
};

-- Current approach: Find the affected cluster, remove the link, select all remaining links,
-- reset singletons, reinsert all remaining links, delete clusterless singletons
create procedure DBpediaRemoveLinkFromClustering
(
	in tableNamePrefix VARCHAR,
	in linkToRemove BIGINT
)
{
	DECLARE linkMap, viewMap VARCHAR;
	linkMap := sprintf('%s_links', tableNamePrefix);
	viewMap := sprintf('%s_view', tableNamePrefix);

	DECLARE state, msg, descs, rows, singletonRows ANY;
	DECLARE clusterToUpdate, it BIGINT;

	-- Find the affected cluster
	EXEC(sprintf('SELECT ClusterId FROM %s WHERE LinkId = %i', linkMap, linkToRemove), state, msg, vector(), 1, descs, rows);

	-- assign cluster id if already exists
	IF (LENGTH(rows) > 0)
	{
		clusterToUpdate := CAST(rows[0][0] as BIGINT);
	}

	-- We have a cluster to update
	if(clusterToUpdate > 0)
	{
		-- Delete the link
		EXEC(sprintf('DELETE FROM %s WHERE LinkId = %i', linkMap, linkToRemove));
		
		-- Select the remaining links
		EXEC(sprintf('SELECT LinkId FROM %s WHERE ClusterId = %i', linkMap, clusterToUpdate), state, msg, vector(), 0, descs, rows);

		-- Remove the links
		EXEC(sprintf('DELETE FROM %s WHERE ClusterId = %i', linkMap, clusterToUpdate));
		
		-- Remove the singletons
		EXEC(sprintf('DELETE FROM %s WHERE ClusterId = %i', viewMap, clusterToUpdate));

		-- Re-Insert the links
		for(it := 0; it < LENGTH(rows); it := it + 1)
		{

			EXEC(sprintf('SELECT SingletonId1, SingletonId2 FROM DBpediaLinkMap WHERE LinkId = %i', rows[it][0]), 
				state, msg, vector(), 1, descs, singletonRows);

			--SIGNAL('DEBUG', sprintf('link %s, s1 %s, s2 %s', CAST(rows[it][0] AS VARCHAR), CAST(singletonRows[0][0] AS VARCHAR), CAST(singletonRows[0][1] AS VARCHAR)));
			DBPediaAddLinkToCluster(rows[it][0], singletonRows[0][0], singletonRows[0][1], linkMap, viewMap);
		}
	}
};

-- Adds a link to a clustering. Any semi-reliable cluster id stabilization was removed in this process, since it will be applied
-- later as a reliable post processing step.
create procedure DBPediaAddLinkToCluster
(
	in linkToInsert BIGINT,
	in singletonId1 BIGINT,
	in singletonId2 BIGINT,
	in linkMap VARCHAR,
	in viewMap VARCHAR
)
{
	-- Declare variables for the select
	DECLARE state, msg, descs, rows ANY;
	DECLARE clusterId1, clusterId2, major, minor, linkId BIGINT;

	-- select the cluster ids for the first singleton
	EXEC(sprintf('SELECT ClusterId FROM %s WHERE SingletonId = %i', viewMap, singletonId1), state, msg, vector(), 1, descs, rows);

	-- assign cluster id if already exists
	IF (LENGTH(rows) > 0)
	{
		clusterId1 := CAST(rows[0][0] as BIGINT);
	}

	-- once again for the second singleton
	EXEC(sprintf('SELECT ClusterId FROM %s WHERE SingletonId = %i', viewMap, singletonId2), state, msg, vector(), 1, descs, rows);
	
	-- assign cluster id if already exists
	IF (LENGTH(rows) > 0)
	{
		clusterId2 := CAST(rows[0][0] as BIGINT);
	}

	-- There is already a cluster for singleton id 1 and singleton id 2
	IF ((clusterId1 <> 0) AND (clusterId2 <> 0)) 
	{
		-- The cluster ids differ, so two clusters get merged!
		IF (clusterId1 <> clusterId2) 
		{
			-- Replace all cluster entries of one cluster id with the other
			-- This is the most expensive operation but should only occur rarely or when removing a link
			EXEC(sprintf('UPDATE %s SET ClusterId = %i WHERE ClusterId = %i', viewMap, clusterId1, clusterId2));
			EXEC(sprintf('UPDATE %s SET ClusterId = %i WHERE ClusterId = %i', linkMap, clusterId1, clusterId2));
			EXEC(sprintf('INSERT INTO %s(LinkId, ClusterId) values(%i, %i)', linkMap, linkToInsert, clusterId1));
		}

		-- If the cluster ids don't differ, no further step is required
	}
	-- There are no clusters for either singleton id 1 or singleton id 2, create a new one!
	ELSE IF ((clusterId1 = 0) AND (clusterId2 = 0)) 
	{
		-- Insert both singletons into the clustering with any of the two as cluster id
		EXEC(sprintf('INSERT INTO %s(SingletonId, ClusterId) values(%i, %i)', viewMap, singletonId1, singletonId1));
		EXEC(sprintf('INSERT INTO %s(SingletonId, ClusterId) values(%i, %i)', viewMap, singletonId2, singletonId1));
		EXEC(sprintf('INSERT INTO %s(LinkId, ClusterId) values(%i, %i)', linkMap, linkToInsert, singletonId1));
	}
	ELSE IF (clusterId1 = 0) 
	{
		-- Only singleton id 1 doesn't have a cluster, singleton id 2 already has one
		EXEC(sprintf('INSERT INTO %s(SingletonId, ClusterId) values(%i, %i)', viewMap, singletonId1, clusterId2));
		EXEC(sprintf('INSERT INTO %s(LinkId, ClusterId) values(%i, %i)', linkMap, linkToInsert, clusterId2));
	}
	ELSE IF (clusterId2 = 0) 
	{
		-- Only singleton id 2 doesn't have a cluster, singleton id 1 already has one
		EXEC(sprintf('INSERT INTO %s(SingletonId, ClusterId) values(%i, %i)', viewMap, singletonId2, clusterId1));
		EXEC(sprintf('INSERT INTO %s(LinkId, ClusterId) values(%i, %i)', linkMap, linkToInsert, clusterId1));
	}	

	commit work;
};

-- New links will be fed into the database with this procedure. A link connects two uris and has
-- a specified relation such as "sameAs"
create procedure DBpediaInsertLink
(
	in uri1 VARCHAR,
	in rel VARCHAR,
	in uri2 VARCHAR
)
{
	-- Don't insert weird links
	IF((uri1 IS NULL) OR (uri2 IS NULL)) 
	{
		RETURN;
	}

	-- Make the singleton id counter ready. We were not sure, if the same thing can be solved for singleton ids by 
	-- using IDENTITY, since it was unclear how IDENTITY behaves with BIGINTs. It seemed to wrap just like a normal INTEGER
	DECLARE singletonIdCounter, singletonId1, singletonId2 BIGINT;
	singletonIdCounter := (SELECT Counter FROM DBpediaIdCounter);

	DECLARE dbpediaId1, dbpediaId2 VARCHAR;
	
	-- uris will be parsed into a unified format here, for debugging, the uris will be kept unchanged.
	dbpediaId1 := uri1; --'http://dbpedia.org/global/' || CAST(encode_base64(DBpediaNormalizeIri(uri1)) as VARCHAR);
	dbpediaId2 := uri2; --'http://dbpedia.org/global/' || CAST(encode_base64(DBpediaNormalizeIri(uri2)) as VARCHAR);

	-- Singleton ids are selected from the singleton map, where they might have an entry already
	singletonId1 := (SELECT SingletonId FROM DBpediaSingletonMap WHERE DBpediaId = dbpediaId1);
	singletonId2 := (SELECT SingletonId FROM DBpediaSingletonMap WHERE DBpediaId = dbpediaId2);

	-- If there is no entry for the first singleton id, create a new one using the singletonIdCounter
	IF(singletonId1 IS NULL) 
	{
		singletonId1 := singletonIdCounter;
		INSERT INTO DBpediaSingletonMap(DBpediaId, SingletonId) values(dbpediaId1, singletonId1);
		singletonIdCounter := singletonIdCounter + 1;
	}

	-- Same thing for the second id
	IF(singletonId2 IS NULL) 
	{
		singletonId2 := singletonIdCounter;
		INSERT INTO DBpediaSingletonMap(DBpediaId, SingletonId) values(dbpediaId2, singletonId2);
		singletonIdCounter := singletonIdCounter + 1;
	}
	
	-- Always insert the link into the link map 
	INSERT INTO DBpediaLinkMap(SingletonId1, SingletonId2, Relation, InsertDate) 
		VALUES(singletonId1, singletonId2, rel, now());

	-- The increased bigint counter is saved back into its table
	UPDATE DBpediaIdCounter SET Counter = singletonIdCounter;
};

-- callback function for csv_parse containing cells, row index and list of excepted predicates (userdata)
-- since we 'misuse' the csv bulk load the first char of every cell is always a '<' and has to be substringed
-- csv_parse: http://docs.openlinksw.com/virtuoso/fn_csv_parse/
-- example call: here we open a gz compressed turtle file (make sure to put these in a 'dirs-allowed' directory -> ini config)
-- present the callback function, define the userdata (which is a vecor of allowed predicates), and define a range (from, to; to read the whole file: 0, null)
-- the last argument overrides the default deliminator of the csv, we use the closing '>' which partitions the tutle line, but leaves the opening '<' in place

-- log_enable(2);
-- csv_parse(gz_file_open('/home/mf/virtuoso/dumpfiles/sameas_all_wikis_wikidata.ttl.gz'), 'DB.DBA.DBpediaBulkLoadTurtleFile', vector('http://www.w3.org/2002/07/owl#sameAs'), 0, null, vector('csv-delimiter', '>', 'csv-quote', '"'));
create procedure DBpediaBulkLoadTurtleFile(inout cells any, in ind int, inout predicates any){
	-- subj, pred, obj 

    if(LENGTH(cells) >= 3 AND position(substring(cells[1], 1, LENGTH(cells[1])), predicates) >=0) {
		DBpediaInsertLink(subseq(cells[0], 1), subseq(cells[1], 1), subseq(cells[2], 1));
    }
};

GRANT EXECUTE ON DB.DBA.DBpediaRemoveLink TO "SPARQL";
GRANT EXECUTE ON DB.DBA.DBpediaInsertLink TO "SPARQL";
GRANT EXECUTE ON DB.DBA.DBpediaSelectCluster TO "SPARQL";
GRANT EXECUTE ON DB.DBA.DBPediaCreateClusterByDate TO "SPARQL";
GRANT EXECUTE ON DB.DBA.DBPediaCreateRedirectMap TO "SPARQL";

INSERT INTO DBpediaIdCounter(Counter) values(100);


-- FOR TESTING --

-- Parse the links into the database, link to .ttl file has to be adjusted, parameter '100000' is the max amount of links to parse in

log_enable(2); 
checkpoint_interval(0); 
csv_parse(gz_file_open('C:/Users/Jan/Desktop/dump/sameas_all_wikis_wikidata.ttl'), 'DB.DBA.DBpediaBulkLoadTurtleFile', vector('http://www.w3.org/2002/07/owl#sameAs'), 0, 1000, vector('csv-delimiter', '>', 'csv-quote', '"'));

-- Insert a random wrong link between two singletons
INSERT INTO DBpediaLinkMap(SingletonId1, SingletonId2, Relation, InsertDate) VALUES(100, 400, 'sameAs', now());

-- Drop the dynamic test tables
DROP TABLE clustering_001_links;
DROP TABLE clustering_001_view;
DROP TABLE clustering_001_history;

DROP TABLE clustering_002_links;
DROP TABLE clustering_002_view;
DROP TABLE clustering_002_history;

-- Create a new clustering for all inserted links:
-- This will create two tables: clustering_001_links and clustering_001_view. 
-- clustering_001_links will contain all the links added to the clustering, together with the cluster each link is currently in
-- clustering_001_view will contain the singleton-to-cluster map
-- Because of the wrong link inserted above, two separate clusters will be merged into one
SELECT DB.DBA.DBPediaCreateClusterByDate('clustering_001', '2010-01-01 00:00:00.000000', '2020-01-01 00:00:00.000000');	
SELECT DB.DBA.DBPediaCreateClusterByDate('clustering_002', '2010-01-01 00:00:00.000000', '2020-01-01 00:00:00.000000');	

-- Remove the wrong link from one clustering
SELECT DB.DBA.DBpediaRemoveLinkFromClustering('clustering_001', 1001);

SELECT DB.DBA.DBPediaCreateRedirectMap('clustering_001', 'clustering_002');

-- Clustering is back to normal!
SELECT * FROM clustering_001_view;


