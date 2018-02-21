SELECT DB.DBA.DBpediaInsertLink('http://dbpedia.org/page/Kentish_ragstone','http://www.wikidata.org/entity/Q63920340');
SELECT DB.DBA.DBpediaInsertLink('http://dbpedia.org/page/Kentish_ragstone','http://rdf.freebase.com/ns/m.0gysfm83');
SELECT DB.DBA.DBpediaInsertLink('http://dbpedia.org/page/Kentish_ragstone','http://WRONG');
SELECT DB.DBA.DBpediaInsertLink('http://WRONG','http://dbpedia.org/page/test');
SELECT DB.DBA.DBpediaInsertLink('http://dbpedia.org/page/test','http://www.wikidata.org/entity/Q63sfasdf920340');
SELECT DB.DBA.DBpediaInsertLink('http://dbpedia.org/page/test','http://rdf.freebase.com/ns/m.0gysfm83dghdfgh');
SELECT DB.DBA.DBpediaInsertLink('http://dbpedia.org/page/test','http://www.wikidata.org/entity/Qfdgsdfgsdfg63920340');
SELECT DB.DBA.DBpediaInsertLink('http://dbpedia.org/page/test','http://rdf.freebase.com/ns/m.0gysghkhjkkkkkkkfm83');

SELECT DB.DBA.DBpediaRemoveLink(4);

SELECT * FROM DBpediaSingletonMap;
SELECT * FROM DBpediaLinkMap;
SELECT * FROM DBpediaIdCounter;
SELECT * FROM DBpediaCurrentClusteringView;


SELECT DB.DBA.DBpediaSelectCluster('http://aa.dbpedia.org/resource/Wikipedia');
SELECT DB.DBA.DBpediaInsertLink('test1', 'test2');


SELECT SingletonId FROM DBpediaSingletonMap WHERE DBpediaId = 'http://aa.dbpedia.org/resource/Wikipedia'

SELECT ClusterId FROM DBpediaCurrentClusteringView WHERE SingletonId = (SELECT SingletonId FROM DBpediaSingletonMap WHERE DBpediaId = 'http://aa.dbpedia.org/resource/Wikipedia');




SELECT * FROM DBpediaSingletonMap 
WHERE SingletonId = (SELECT ClusterId FROM DBpediaCurrentClusteringView 
WHERE SingletonId = (SELECT SingletonId FROM DBpediaSingletonMap
WHERE DBpediaId = 'http://aa.dbpedia.org/resource/Wikipedia'));

SELECT * FROM DBpediaCurrentClusteringView 
WHERE ClusterId = (SELECT ClusterId FROM DBpediaCurrentClusteringView 
WHERE SingletonId = (SELECT SingletonId FROM DBpediaSingletonMap
WHERE DBpediaId = 'http://aa.dbpedia.org/resource/Wikipedia'));


SELECT * FROM
(SELECT * FROM DBpediaCurrentClusteringView 
WHERE ClusterId = (SELECT ClusterId FROM DBpediaCurrentClusteringView 
WHERE SingletonId = (SELECT SingletonId FROM DBpediaSingletonMap
WHERE DBpediaId = 'http://aa.dbpedia.org/resource/Wikipedia'))) slct 
LEFT OUTER JOIN DBpediaSingletonMap map ON slct.SingletonId = map.SingletonId ORDER BY Confidence DESC;



SELECT csv_parse(gz_file_open('C:/Users/Jan/Desktop/dump/sameas_all_wikis_wikidata.ttl'), 'DB.DBA.DBpediaBulkLoadTurtleFile', vector('http://www.w3.org/2002/07/owl#sameAs'), 500000, null, vector('csv-delimiter', '>', 'csv-quote', '"'));


log_enable(2);          -- disable logging
checkpoint_interval(0); -- disable checkpoints
csv_parse(gz_file_open('C:/Users/Jan/Desktop/dump/sameas_all_wikis_wikidata.ttl'), 'DB.DBA.DBpediaBulkLoadTurtleFile', vector('http://www.w3.org/2002/07/owl#sameAs'), 0, 1000000, vector('csv-delimiter', '>', 'csv-quote', '"'));
SELECT DB.DBA.DBPediaAddLinkRangeToCluster('2010-01-01 00:00:00.000000', '2020-01-01 00:00:00.000000', 10000000);



SELECT DB.DBA.DBPediaCreateClusterByDate('clustering_001', '2010-01-01 00:00:00.000000', '2020-01-01 00:00:00.000000');	
SELECT * FROM clustering_010_links
SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME='clustering_001_view'


log_enable(2);          -- disable logging
checkpoint_interval(0); -- disable checkpoints
csv_parse(gz_file_open('C:/Users/Jan/Desktop/dump/sameas_all_wikis_wikidata.ttl'), 'DB.DBA.DBpediaBulkLoadTurtleFile', 
	vector('http://www.w3.org/2002/07/owl#sameAs'), 0, 1000, vector('csv-delimiter', '>', 'csv-quote', '"'));