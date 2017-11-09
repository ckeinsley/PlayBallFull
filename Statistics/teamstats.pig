records = LOAD '/user/root/PlayBall/GameLogs/GL$year.TXT' USING PigStorage(',');

minHomeRecords = FOREACH records GENERATE $6, $10, $9, $18, $17;
minVisitingRecords = FOREACH records GENERATE $3, $9, $10, $18, $17;
teamRecords = UNION minHomeRecords, minVisitingRecords;

winRecords = FILTER teamRecords BY $1 > $2;
wingroup = GROUP winRecords BY $0;
winCount = FOREACH wingroup GENERATE group, COUNT(winRecords) AS wins;
loseRecords = FILTER teamRecords BY $2 > $1;
losegroup = GROUP loseRecords BY $0;
loseCount = FOREACH losegroup GENERATE group, COUNT(loseRecords) AS loses;
winloserecords = JOIN winCount BY group, loseCount BY group;

groupRecords = GROUP teamRecords BY $0;
stats = FOREACH groupRecords GENERATE group, SUM(teamRecords.$1) AS runs, COUNT(teamRecords) AS games, AVG(teamRecords.$3) AS avgTime, AVG(teamRecords.$4) AS avgAttendance;

joinResults = JOIN stats BY group, winloserecords BY winCount::group;
final = FOREACH joinResults GENERATE stats::group, runs, games, wins, loses, avgTime, avgAttendance;
STORE final INTO 'hbase://teamstats$year' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage(
'stats:runs
 stats:games
 stats:wins
 stats:loses
 stats:avgTime
 stats:avgAttendance'
);