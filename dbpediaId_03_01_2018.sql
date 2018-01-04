drop table DBpediaSingletonMap;
drop table DBpediaLinkMap;
drop table DBpediaHistoryMap;
drop table DBpediaIdCounter;
drop table DBpediaCurrentClusteringView;
drop table DBpediaCurrentClusteringLinks;
drop table SourceMap;

create table DBpediaSingletonMap
(
	"DBpediaId" VARCHAR PRIMARY KEY,
	"SingletonId" BIGINT,
	"Confidence" FLOAT
);

create table SourceMap
(
	"SourceId" BIGINT IDENTITY,
	"SourceName" VARCHAR,
	"SourcePrefix" VARCHAR,
	"Confidence" FLOAT
);

create table DBpediaIdCounter
(
	"Counter" BIGINT NOT NULL
);

create table DBpediaLinkMap
(
	"LinkId" BIGINT IDENTITY,
	"SingletonId1" BIGINT NOT NULL,
	"Confidence1" FLOAT,
	"SingletonId2" BIGINT NOT NULL,
	"Confidence2" FLOAT,
	"InsertDate" DATETIME
);

create table DBpediaHistoryMap
(
	"ActionId" BIGINT IDENTITY,
	"ActionType" VARCHAR NOT NULL, 
	"LinkId" BIGINT NOT NULL, 
	"SingletonId1" BIGINT NOT NULL,
	"SingletonId2" BIGINT NOT NULL
);

create table DBpediaCurrentClusteringView
(
	"SingletonId" BIGINT PRIMARY KEY,
	"ClusterId" BIGINT NOT NULL
);

--create index clusterIndex on DBpediaCurrentClusteringView ( ClusterId ASC );

create table DBpediaCurrentClusteringLinks
(
	"LinkId" BIGINT PRIMARY KEY,
	"ClusterId" BIGINT NOT NULL
);


commit work;
delay(1);





--used to normalize input uri
create procedure
DBpediaNormalizeIri
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

create procedure
DBPediaAddLinkRangeToCluster
(
	in fromDate DATETIME,
	in toDate DATETIME,
	in maxId INTEGER
)
{


	FOR SELECT LinkId, SingletonId1, Confidence1, SingletonId2, Confidence2 FROM DBpediaLinkMap WHERE InsertDate > fromDate AND InsertDate < toDate
		AND LinkId < maxId DO {

		DBPediaAddLinkToCluster(LinkId, SingletonId1, Confidence1, SingletonId2, Confidence2);
	}
};


create procedure
DBPediaAddLinkToCluster
(
	in linkToInsert BIGINT,
	in singletonId1 BIGINT,
	in confidence1 FLOAT,
	in singletonId2 BIGINT,
	in confidence2 FLOAT
)
{


	DECLARE clusterId1, clusterId2, major, minor, linkId BIGINT;

	linkId := (SELECT LinkId FROM DBpediaCurrentClusteringLinks WHERE LinkId = linkToInsert);

	IF(linkId IS NOT NULL) {
		RETURN;
	}

	clusterId1 := (SELECT ClusterId FROM DBpediaCurrentClusteringView WHERE SingletonId = singletonId1);
	clusterId2 := (SELECT ClusterId FROM DBpediaCurrentClusteringView WHERE SingletonId = singletonId2);

	IF ((clusterId1 IS NOT NULL) AND (clusterId2 IS NOT NULL)) 
	{
		IF (clusterId1 <> clusterId2) 
		{
			if(confidence1 <> confidence2)
			{
				IF(confidence1 > confidence2) 
				{
					major := clusterId1;
					minor := clusterId2;
				}
				ELSE
				{
					minor := clusterId1;
					major := clusterId2;
				}
			}
			ELSE
			{
				IF(clusterId1 < clusterId2) 
				{
					major := clusterId1;
					minor := clusterId2;
				}
				ELSE
				{
					minor := clusterId1;
					major := clusterId2;
				}
			}


			UPDATE DBpediaCurrentClusteringView SET ClusterId = major WHERE ClusterId = minor;
		}
	}
	ELSE IF ((clusterId1 IS NULL) AND (clusterId2 IS NULL)) {

		IF (confidence1 <> confidence2)
		{
			IF(confidence1 > confidence2) 
			{
				major := singletonId1;
			}
			ELSE
			{
				major := singletonId2;
			}
		}
		ELSE
		{
			IF(clusterId1 < clusterId2) 
			{
				major := singletonId1;
			}
			ELSE
			{
				major := singletonId2;
			}
		}

		INSERT INTO DBpediaCurrentClusteringView(SingletonId, ClusterId) values(singletonId1, major);
		INSERT INTO DBpediaCurrentClusteringView(SingletonId, ClusterId) values(singletonId2, major);
	}
	ELSE IF (clusterId1 IS NULL) 
	{
		clusterId1 := singletonId1;

		IF (confidence1 > confidence2 OR ((confidence1 = confidence2) AND (clusterId1 < clusterId2)))
		{
			INSERT INTO DBpediaCurrentClusteringView(SingletonId, ClusterId) values(singletonId1, clusterId1);
			UPDATE DBpediaCurrentClusteringView SET ClusterId = clusterId1 WHERE ClusterId = clusterId2;
		}
		ELSE
		{
			INSERT INTO DBpediaCurrentClusteringView(SingletonId, ClusterId) values(singletonId1, clusterId2);
		}
	}
	ELSE IF (clusterId2 IS NULL) 
	{
		clusterId2 := singletonId2;

		IF (confidence2 > confidence1 OR ((confidence1 = confidence2) AND (clusterId2 < clusterId1)))
		{
			INSERT INTO DBpediaCurrentClusteringView(SingletonId, ClusterId) values(singletonId2, clusterId2);
			UPDATE DBpediaCurrentClusteringView SET ClusterId = clusterId2 WHERE ClusterId = clusterId1;
		}
		ELSE
		{
			INSERT INTO DBpediaCurrentClusteringView(SingletonId, ClusterId) values(singletonId2, clusterId1);
		}
	}

	-- Always insert the link into the link map 
	INSERT INTO DBpediaCurrentClusteringLinks(LinkId, ClusterId) VALUES(linkToInsert, singletonId1);

	commit work;
};

-- Used to insert a link, updates the clustering 
create procedure
DBpediaInsertLink
(
	in uri1 VARCHAR,
	in uri2 VARCHAR
)
{
	if(uri1 IS NULL OR uri2 IS NULL)
		SIGNAL('TODO', 'TODO');

	DECLARE singletonIdCounter BIGINT;
	singletonIdCounter := (SELECT Counter FROM DBpediaIdCounter);

	DECLARE dbpediaId1, dbpediaId2 VARCHAR;
	DECLARE singletonId1, singletonId2 BIGINT;
	DECLARE confidence1, confidence2 FLOAT;
	
	dbpediaId1 := uri1; --'http://dbpedia.org/global/' || CAST(encode_base64(DBpediaNormalizeIri(uri1)) as VARCHAR);
	dbpediaId2 := uri2; --'http://dbpedia.org/global/' || CAST(encode_base64(DBpediaNormalizeIri(uri2)) as VARCHAR);

	singletonId1 := (SELECT SingletonId FROM DBpediaSingletonMap WHERE DBpediaId = dbpediaId1);
	singletonId2 := (SELECT SingletonId FROM DBpediaSingletonMap WHERE DBpediaId = dbpediaId2);
	confidence1 := (SELECT Confidence FROM DBpediaSingletonMap WHERE DBpediaId = dbpediaId1);
	confidence2 := (SELECT Confidence FROM DBpediaSingletonMap WHERE DBpediaId = dbpediaId2);

	IF(singletonId1 IS NULL) {

		confidence1 := (SELECT Confidence FROM SourceMap WHERE uri1 LIKE SourcePrefix );

		if(confidence1 IS NULL) {
			confidence1 := 0.0;
		}

		singletonId1 := singletonIdCounter;
		INSERT INTO DBpediaSingletonMap(DBpediaId, SingletonId, Confidence) values(dbpediaId1, singletonId1, confidence1);
		singletonIdCounter := singletonIdCounter + 1;
	}

	IF(singletonId2 IS NULL) {

		confidence2 := (SELECT Confidence FROM SourceMap WHERE uri2 LIKE SourcePrefix );

		if(confidence2 IS NULL) {
			confidence2 := 0.0;
		}

		singletonId2 := singletonIdCounter;
		INSERT INTO DBpediaSingletonMap(DBpediaId, SingletonId, Confidence) values(dbpediaId2, singletonId2, confidence2);
		singletonIdCounter := singletonIdCounter + 1;
	}
	
	-- Always insert the link into the link map 
	INSERT INTO DBpediaLinkMap(SingletonId1, Confidence1, SingletonId2, Confidence2, InsertDate) 
		VALUES(singletonId1, confidence1, singletonId2, confidence2, now());

	-- Cluster ids are not the same whenever a link led to a new clustering
	UPDATE DBpediaIdCounter SET Counter = singletonIdCounter;
};



-- callback function for csv_parse containing cells, row index and list of excepted predicates (userdata)
-- since we 'misuse' the csv bulk load the first char of every cell is always a '<' and has to be substringed
-- csv_parse: http://docs.openlinksw.com/virtuoso/fn_csv_parse/
-- example call: here we open a gz compressed turtle file (make sure to put these in a 'dirs-allowed' directory -> ini config)
-- present the callback function, define the userdata (which is a vecor of allowed predicates), and define a range (from, to; to read the whole file: 0, null)
-- the last argument overrides the default deliminator of the csv, we use the closing '>' which partitions the tutle line, but leaves the opening '<' in place


-- log_enable(2);          -- disable logging
-- checkpoint_interval(0); -- disable checkpoints
-- csv_parse(gz_file_open('/home/mf/virtuoso/dumpfiles/sameas_all_wikis_wikidata.ttl.gz'), 'DB.DBA.DBpediaBulkLoadTurtleFile', vector('http://www.w3.org/2002/07/owl#sameAs'), 0, null, vector('csv-delimiter', '>', 'csv-quote', '"'));
create procedure DBpediaBulkLoadTurtleFile(inout cells any, in ind int, inout predicates any){
-- subj, pred, obj 
    if(LENGTH(cells) >= 3 AND position(substring(cells[1], 1, LENGTH(cells[1])), predicates) >=0) {
		DBpediaInsertLink(subseq(cells[0], 1), subseq(cells[2], 1));
    }
};

GRANT EXECUTE ON DB.DBA.DBpediaRemoveLink TO "SPARQL";
GRANT EXECUTE ON DB.DBA.DBpediaInsertLink TO "SPARQL";
GRANT EXECUTE ON DB.DBA.DBpediaSelectCluster TO "SPARQL";


INSERT INTO DBpediaIdCounter(Counter) values(100);
INSERT INTO SourceMap(SourceId, SourceName, SourcePrefix, Confidence) values(1, 'Wikidata', 'http://wikidata%', 1.1);
INSERT INTO SourceMap(SourceId, SourceName, SourcePrefix, Confidence) values(2, 'DBpedia', 'http://dbpedia.org%', 1.0);
