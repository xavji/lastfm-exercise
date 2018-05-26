To build an uber-jar to run the jobs in Spark: ```sbt assembly```
By default the assembly task also runs the tests.

This project contains three functional flavours of Spark jobs:
* jobs that count the number of distinct tracks played by each user:
    * these jobs take exactly two command line arguments:
        1. path to input TSV file downloaded
        1. path to output directory where CSV files with the results are generated
    * RDD version in `lastfm.DistinctSongsByUserRddJob`
    * SQL version in `lastfm.DistinctSongsByUserSqlJob`
    * example: ```spark-submit --master local[*] --class lastfm.DistinctSongsByUserSqlJob  /project-dir/lastfm-exercise-assembly-0.1.0-SNAPSHOT.jar /data/userid-timestamp-artid-artname-traid-traname.tsv /out/distinct-songs```
* jobs that rank the tracks by number of plays and return the top N artist & track name:
    * these jobs take exactly three command line arguments:
        1. path to input TSV file downloaded
        1. path to output directory where CSV files with the results are generated
        1. maximum song rank to return in the results
    * RDD version in `lastfm.PopularSongsRddJob`
    * SQL version in `lastfm.PopularSongsSqlJob`
    * example: ```spark-submit --master local[*] --class lastfm.PopularSongsSqlJob  /project-dir/lastfm-exercise-assembly-0.1.0-SNAPSHOT.jar /data/userid-timestamp-artid-artname-traid-traname.tsv /out/popular 100```
* jobs that find the N longest user sessions (in terms of duration)
    * these jobs take exactly three command line arguments:
        1. path to input TSV file downloaded
        1. path to output directory where CSV files with the results are generated
        1. maximum session duration rank to return in the results
    * RDD version in `lastfm.LongestSessionsRddJob`
    * SQL version in `lastfm.LongestSessionsSqlJob`
    * example: ```spark-submit --driver-memory 4G --master local[*] --class lastfm.LongestSessionsRddJob  /project-dir/lastfm-exercise-assembly-0.1.0-SNAPSHOT.jar /data/userid-timestamp-artid-artname-traid-traname.tsv /out/sessions 10```
    
The output of the longest sessions job is included in `sessions.csv`.
Due to a lack of time, the SQL version of the longest sessions job has not been able to process the complete dataset, probably because of the immutable data structures used during the aggregation.     