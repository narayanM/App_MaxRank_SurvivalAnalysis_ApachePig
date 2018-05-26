
Task 1: To Calculate the MAXimum rank among Apps, over the weeks:



REGISTER /home/training/Desktop/1pig/pig-0.7.0/contrib/piggybank/piggybank.jar

W2 = LOAD 'week2.txt'; // load week 2 Data. 
W3 = LOAD 'week3.txt'; // load week 3 Data.

	// Extract only needed columns from both excel sheets. I.e column 0 and 3 are extracted.
W2_RankGeneral = FOREACH W2 GENERATE $0, $3; 
W3_RankGeneral = FOREACH W3 GENERATE $0, $3; 

	// Above two results are joined by column 0. I.e. joined by ID.
W23 = JOIN W2_RankGeneral BY $0, W3_RankGeneral BY $0; 

	// Filter the rows with an AppID.
W23_NotNull = FILTER A BY appID is not null;

	//  Max Rank of Week 2 and Week 3:
MaxRankWeek23 = FOREACH W23_NotNull GENERATE appID, org.apache.pig.piggybank.evaluation.math.MAX ( rankW3, rankW2 ); 

STORE MaxRankWeek23 into 'MaxRankWeek23.txt'; 

	// Similarly we can scale up for ranks of 3 weeks. And even more.
MaxRankWeek234 = FOREACH A2 GENERATE appID, org.apache.pig.piggybank.evaluation.math.MAX ( rankW4, org.apache.pig.piggybank.evaluation.math.MAX ( rankW3, rankW2 ) ); 



##############################################################



Task 2: To Calculate the Survival Time calculations of the Apps, over the weeks:



W2 = Load 'w2.txt';
W3 = Load 'w3.txt';

FFW2_RankCate = FOREACH FFW2 GENERATE $0, $3; 
FFW3_RankCate = FOREACH FFW3 GENERATE $0, $3; 

FFW23 = JOIN FFW2_RankCate BY $0, FFW3_RankCate BY $0; 

FFW23_RankCate = FOREACH FFW23 GENERATE $0, $1, $3;

STORE FFW23_RankCate into 'FFW23_RankCate.txt'; 
	// copyToLocal and replace all " with null, for the numeric operations to work.

FFW23_RankCate_Load = Load 'FFW23_RankCate1.txt' AS (appID, rankW2, rankW3);

FFW23_SurvivalTime100_1 = FILTER FFW23_RankCate_Load By rankW2 < 100;
FFW23_SurvivalTime100_2 = FILTER FFW23_SurvivalTime100_1 By rankW3 < 100;    // all apps with rank less than 100, in week2 and week3.

STORE FFW23_SurvivalTime100_2 into 'FFW23_SurvivalTime100.txt';

FFW23_SurvivalTime200_1 = FILTER FFW23_RankCate_Load By rankW2 > 100;
FFW23_SurvivalTime200_2 = FILTER FFW23_SurvivalTime200_1 By rankW2 < 200;
FFW23_SurvivalTime200_3 = FILTER FFW23_SurvivalTime200_2 By rankW3 > 100;
FFW23_SurvivalTime200_4 = FILTER FFW23_SurvivalTime200_3 By rankW3 < 200;    // all apps with rank in bet 100 and 200, in week2 and week3.
	store FFW23_SurvivalTime200_4 into 'FFW23_SurvivalTime200.txt';
	
FFW23_SurvivalTime300_1 = FILTER FFW23_RankCate_Load By rankW2 > 200;
FFW23_SurvivalTime300_2 = FILTER FFW23_SurvivalTime300_1 By rankW2 < 300;
FFW23_SurvivalTime300_3 = FILTER FFW23_SurvivalTime300_2 By rankW3 > 200;
FFW23_SurvivalTime300_4 = FILTER FFW23_SurvivalTime300_3 By rankW3 < 300;    // all apps with rank in bet 200 and 300, in week2 and week3.
	store FFW23_SurvivalTime300_4 into 'FFW23_SurvivalTime300.txt';

