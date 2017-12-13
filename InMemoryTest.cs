using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace Data_Management_Testing
{
   
    class InMemoryTest
    {
        // The singleton table indexed by the singleton id
        private static Dictionary<string, string> singletonTable = new Dictionary<string, string>();

        // The clustering table indexed by the cluster id
        private static Dictionary<string, List<string>> clusteringTable = new Dictionary<string, List<string>>();

        static void Main(string[] args)
        {
            // Precompiled regex matcher for links
            Regex matcher = new Regex("<(.*?)> <http://www.w3.org/2002/07/owl#sameAs> <(.*?)> .", RegexOptions.Compiled);

            try
            {
                // Test data with ca 5.2 mio links
                // Download: http://downloads.dbpedia.org/2016-10/core-i18n/wikidata/sameas_external_wikidata.ttl.bz2
                using (StreamReader sr = new StreamReader("sameas_external_wikidata.ttl"))
                {
                    // Max links to read - in this case higher than filesize
                    int maxLinks = 10000000;

                    char[] splitter = { ' ' };

                    Console.WriteLine("Inserting links.");

                    for (int i = 0; i < maxLinks; i++)
                    {
                        // Match the line from the ttl file with the regex matcher
                        //MatchCollection matches = matcher.Matches(sr.ReadLine());

                        string[] split = sr.ReadLine().Split(splitter);

                        // The matcher has found the instances of the sameAs relation
                        string uri1 = split[0].Replace("<", "").Replace(">", "");
                        string uri2 = split[2].Replace("<", "").Replace(">", "");

                        // Both uris are inserted into the singleton map ONLY IF no entry exists
                        softInsertToSingletonMap(uri1);
                        softInsertToSingletonMap(uri2);

                        // The clusters of the current uris does not match
                        if (!singletonTable[uri1].Equals(singletonTable[uri2]))
                        {
                            // Both clusters are retrieved from the singleton table
                            string cluster1 = singletonTable[uri1];
                            string cluster2 = singletonTable[uri2];

                            // The cluster sizes are compared
                            bool uri1Major = getClusterSize(cluster1) > getClusterSize(cluster2);

                            // Find major/minor cluster: the major cluster is the bigger cluster, the minor cluster the smaller one
                            string major = uri1Major ? cluster1 : cluster2;
                            string minor = uri1Major ? cluster2 : cluster1;

                            // Merge the two clusters and update singleton table
                            mergeClusters(major, minor);
                        }


                        // Show how many links we have processed
                        if (i % 100000 == 0)
                        {
                            Console.WriteLine(i.ToString("N0", new NumberFormatInfo()
                            {
                                NumberGroupSizes = new[] { 3 },
                                NumberGroupSeparator = "."
                            }) + " links inserted.");
                        }
                    }
                    
                }

            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }

            Console.WriteLine("Done.");
            Console.ReadKey();
        }

        /// <summary>
        /// Merges two clusters and updates the singleton table
        /// </summary>
        /// <param name="major"></param>
        /// <param name="minor"></param>
        private static void mergeClusters(string major, string minor)
        {
            // When merging clusters we have three cases:
            if (clusteringTable.ContainsKey(major))
            {
                if (clusteringTable.ContainsKey(minor))
                {
                    // A) Both clusters already have an entry in the clustering table and contain singletons
                    // Add the singletons of the minor cluster to the major cluster
                    clusteringTable[major].AddRange(clusteringTable[minor]);

                    // Rewrite the cluster of the minor singletons in the singleton table
                    foreach (string singleton in clusteringTable[minor])
                    {
                        singletonTable[singleton] = major;
                    }

                    // Remove the minor cluster from the cluster table
                    clusteringTable.Remove(minor);
                }
                else
                {
                    // B) Only the major cluster already has an entry
                    // Simply add the minor instance to the major cluster
                    clusteringTable[major].Add(minor);

                    // Rewrite the cluster of the minor singleton in the singleton table
                    singletonTable[minor] = major;
                }
            }
            else
            {
                // C) Both clusters have no entry in the clustering table
                // Create a new entry and add both singletons
                clusteringTable.Add(major, new List<string>());
                clusteringTable[major].Add(major);
                clusteringTable[major].Add(minor);

                // Rewrite in the singleton table
                singletonTable[major] = major;
                singletonTable[minor] = major;
            }
        }

        /// <summary>
        /// Returns the cluster size
        /// </summary>
        /// <param name="cluster"></param>
        /// <returns></returns>
        private static int getClusterSize(string cluster)
        {
            // Return 1 if there is no entry in the clustering table
            if (!clusteringTable.ContainsKey(cluster))
            {
                return 1;
            }

            // Return the size of the list in the clustering table
            return clusteringTable[cluster].Count;
        }

        /// <summary>
        /// Insert a singleton into the singleton map ONLY IF there is no entry yet
        /// </summary>
        /// <param name="p"></param>
        private static void softInsertToSingletonMap(string p)
        {
            if (!singletonTable.ContainsKey(p))
            {
                // Add the singleton with cluster set to itself
                singletonTable.Add(p, p);
            }
        }
    }
}
    

