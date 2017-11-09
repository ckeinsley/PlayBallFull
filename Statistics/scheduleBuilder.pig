REGISTER 'hdfs:///user/root/PlayBall/jar/PlayBallPig-0.0.1-SNAPSHOT.jar';
records = LOAD '/user/root/PlayBall/GameLogs/GL$year.TXT' USING PigStorage(',');

minHomeRecords = FOREACH records GENERATE (chararray)$6 AS team:chararray, (chararray)$0 AS date:chararray, (chararray)$1 AS number:chararray, (chararray)$3 AS oppose:chararray, $10, $9, 'Y';
minVisitingRecords = FOREACH records GENERATE (chararray)$3 AS team:chararray, (chararray)$0 AS date:chararray, (chararray)$1 AS number:chararray, (chararray)$6 AS oppose:chararray, $9, $10, 'N';
teamRecords = UNION minHomeRecords, minVisitingRecords;
keyFinalRecords = FOREACH teamRecords GENERATE CONCAT(edu.rosehulman.gilmordw.TrimQuotes(team), edu.rosehulman.gilmordw.TrimQuotes(date), edu.rosehulman.gilmordw.TrimQuotes(number)), edu.rosehulman.gilmordw.TrimQuotes(oppose), $4, $5, $6;

STORE keyFinalRecords INTO 'hbase://teamschedule' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage(
'sched:opposing
 sched:score
 sched:op_score
 sched:home'
);

