--Make Team table
teams = LOAD '$team' using PigStorage(',');
myteams = FOREACH teams GENERATE $0 as teamId, $2 as city, $3 as name, $1 as division;
STORE myteams INTO 'hbase://teams$year' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage(
'teams_data:city 
 teams_data:name
 teams_data:division'
);

--Make Player table
players = LOAD '$roster' using PigStorage(',');
myplayers = FOREACH players GENERATE $0 as playerId, $5 as teamId, $2 as fname, $1 as lname, $6 as position;
STORE myplayers INTO 'hbase://players$year' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage(
'players_data:teamId 
 players_data:firstname 
 players_data:lastname
 players_data:position'
);
