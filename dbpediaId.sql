drop table DBpediaSingletonMap;
drop table DBpediaLinkMap;
drop table DBpediaHistoryMap;

create table DBpediaSingletonMap
(
	"DBpediaId" VARCHAR PRIMARY KEY,
	"SingletonId" INTEGER IDENTITY,
	"ClusterId" INTEGER NOT NULL
);

create table DBpediaLinkMap
(
	"LinkId" INTEGER IDENTITY,
	"SingletonId1" INTEGER NOT NULL,
	"SingletonId2" INTEGER NOT NULL,
	"ClusterId" INTEGER NOT NULL
);

create table DBpediaHistoryMap
(
	"ActionId" INTEGER IDENTITY,
	"ActionType" VARCHAR NOT NULL, 
	"LinkId" INTEGER NOT NULL, 
	"SingletonId1" INTEGER NOT NULL,
	"SingletonId2" INTEGER NOT NULL
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
	if(uri IS NULL)
		SIGNAL('TODO', 'TODO');

	--TODO check if uri use rfc1808_parse_uri (url) function see: https://github.com/chile12/DataEventFramework/blob/master/VirtuosoSql%20%26%20Doku/CentralDB.sql#L238 ff

	DECLARE cleaned VARCHAR;
	cleaned := TRIM(LOWER(uri));

	--TODO some more normalization??
	return cleaned;
};

-- Used to insert a link, updates the clustering 
create procedure
DBpediaInsertLink
(
	in uri1 VARCHAR,
	in uri2 VARCHAR
)
{
	-- generate dbpedia ids and encode as INTEGER
	DECLARE dbpediaId1, dbpediaId2 VARCHAR;

	dbpediaId1 := 'http://dbpedia.org/global/' || CAST(encode_base64(DBpediaNormalizeIri(uri1)) as VARCHAR);
	dbpediaId2 := 'http://dbpedia.org/global/' || CAST(encode_base64(DBpediaNormalizeIri(uri2)) as VARCHAR);

	--SIGNAL(CAST(dbpediaId1 as VARCHAR), CAST(dbpediaId2 as VARCHAR));
	
	-- Soft insert, only insert if entry doesn't exist
	INSERT SOFT DBpediaSingletonMap(DBpediaId, ClusterId) values(dbpediaId1, 0);
	INSERT SOFT DBpediaSingletonMap(DBpediaId, ClusterId) values(dbpediaId2, 0);

	DECLARE singletonId1, singletonId2, clusterId1, clusterId2, major, minor INTEGER;

	-- Select the newly generated singleton ids
	singletonId1 := (SELECT SingletonId FROM DBpediaSingletonMap WHERE DBpediaId = dbpediaId1);
	singletonId2 := (SELECT SingletonId FROM DBpediaSingletonMap WHERE DBpediaId = dbpediaId2);

	-- Select the cluster ids
	clusterId1 := (SELECT ClusterId FROM DBpediaSingletonMap WHERE DBpediaId = dbpediaId1);
	clusterId2 := (SELECT ClusterId FROM DBpediaSingletonMap WHERE DBpediaId = dbpediaId2);

	-- if the cluster ids haven't been set yet, set them to the singleton id
	if(clusterId1 = 0)
	{
		UPDATE DBpediaSingletonMap SET ClusterId = singletonId1 WHERE DBpediaId = dbpediaId1;
		clusterId1 := singletonId1;
	}
	
	if(clusterId2 = 0)
	{
		UPDATE DBpediaSingletonMap SET ClusterId = singletonId2 WHERE DBpediaId = dbpediaId2;
		clusterId2 := singletonId2;
	}

	-- Compare the singleton ids
	IF (DBpediaCompareSingletonIds(clusterId1, clusterId2)) {
		major := clusterId1;
		minor := clusterId2;
	} ELSE {
		minor := clusterId1;
		major := clusterId2;
	}

	-- Always insert the link into the link map 
	INSERT INTO DBpediaLinkMap(SingletonId1, SingletonId2, ClusterId) VALUES(singletonId1, singletonId2, major);

	-- Cluster ids are not the same whenever a link led to a new clustering
	if(clusterId1 <> clusterId2)
	{
		UPDATE DBpediaSingletonMap SET ClusterId = major WHERE ClusterId = minor;
		UPDATE DBpediaLinkMap SET ClusterId = major WHERE ClusterId = minor;
	}
};



-- re-inserts a link, does NOT re-insert into the link map, only updates the clustering
create procedure
DBpediaReinsertLink
(
	in linkToInsert INTEGER,
	in singletonId1 INTEGER,
	in singletonId2 INTEGER
)
RETURNS INTEGER
{
	-- Select the clusters of the two singleton ids
	DECLARE clusterId1,clusterId2, major, minor INTEGER;
    DECLARE clusterId2 ANY;

    clusterId1 := (SELECT ClusterId FROM DBpediaSingletonMap WHERE SingletonId = singletonId1);
	clusterId2 := (SELECT ClusterId FROM DBpediaSingletonMap WHERE SingletonId = singletonId2);

	-- Compare the singleton ids
	IF (DBpediaCompareSingletonIds(clusterId1, clusterId2)) {
		major := clusterId1;
		minor := clusterId2;
	} ELSE {
		minor := clusterId1;
		major := clusterId2;
	}

	UPDATE DBpediaLinkMap SET ClusterId = major WHERE LinkId = linkToInsert;

	-- Cluster ids are not the same whenever a link led to a new clustering
	if(clusterId1 <> clusterId2)
	{
		UPDATE DBpediaSingletonMap SET ClusterId = major WHERE ClusterId = minor;
		UPDATE DBpediaLinkMap SET ClusterId = major WHERE ClusterId = minor;
	}
};

-- Removes a link from the link map and updates the clustering
create procedure
DBpediaRemoveLink
( 
	in linkIdToDelete INTEGER
)
{
	-- Select the cluster of the deletee
	DECLARE clusterId ANY;
	clusterId := (SELECT ClusterId FROM DBpediaLinkMap WHERE LinkId = linkIdToDelete);

	-- Delete the link from the link map
	DELETE FROM DBpediaLinkMap WHERE LinkId = linkIdToDelete;

	-- Reset all singletons of the affected cluster
	UPDATE DBpediaSingletonMap SET ClusterId = SingletonId WHERE ClusterId = clusterId;

	-- Re-insert all the remaining links of the affected cluster
	FOR SELECT LinkId, SingletonId1, SingletonId2 FROM DBpediaLinkMap WHERE ClusterId = clusterId DO {

		DBpediaReinsertLink(LinkId, SingletonId1, SingletonId2);
	}
};

-- Verifies if 'major' is really bigger than 'minor', returns 1 or 0
create procedure
DBpediaCompareSingletonIds
(
	in major INTEGER,
	in minor INTEGER
)
{
	DECLARE countMajor, countMinor INTEGER;

	countMajor := (SELECT COUNT(*) FROM DBpediaSingletonMap WHERE ClusterId = major);
	countMinor := (SELECT COUNT(*) FROM DBpediaSingletonMap WHERE ClusterId = minor);

	IF(countMajor > countMinor)
		RETURN 1;
	
	IF(countMajor < countMinor)
		RETURN 0;


	IF(major < minor) 
		RETURN 1;
	ELSE 
		RETURN 0;
};

GRANT EXECUTE ON DB.DBA.DBpediaRemoveLink TO "SPARQL";
GRANT EXECUTE ON DB.DBA.DBpediaInsertLink TO "SPARQL";