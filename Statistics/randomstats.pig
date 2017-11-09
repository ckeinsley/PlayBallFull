records = LOAD '/user/root/PlayBall/PlayByPlay/$year/' USING PigStorage(',');
infoRecords = FILTER records BY $0 == 'info';
subRecords = FILTER records BY $0 == 'sub';
windSpeedRecords = FILTER infoRecords BY $1 == 'windspeed';
windSpeedGroup = GROUP windSpeedRecords BY $0;
windSpeedSpeedGroup = GROUP windSpeedRecords BY $2;
windCount = FOREACH windSpeedSpeedGroup GENERATE 'const', group, COUNT(windSpeedRecords) AS spCount;
windCountNonZero = FILTER windCount BY group != 0;
windCountGroup = GROUP windCountNonZero BY $0;
windCountMax = FOREACH windCountGroup GENERATE MAX(windCountNonZero.spCount), '$year';
windMode = JOIN windCountMax BY $0, windCount BY spCount;
windGen = FOREACH windSpeedGroup GENERATE '$year', MAX(windSpeedRecords.$2) AS max_windspeed, MIN(windSpeedRecords.$2) AS min_windspeed, AVG(windSpeedRecords.$2) AS avg_windspeed;
windFinal = JOIN windMode by $1, windGen by $0;
result = FOREACH windFinal GENERATE windFinal.$1, group, windFinal.windGen::max_windspeed, windFinal.windGen::min_windspeed, windFinal.windGen::avg_windspeed;
STORE result INTO 'hbase://windstats' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage(
'stats:mode
 stats:max
 stats:min
 stats:avg'
);