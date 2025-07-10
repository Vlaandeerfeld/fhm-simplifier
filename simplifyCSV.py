#Files exported as parquet format. Change to_parquet function to to_csv function to export files as csv instead.

#Silence warnings because below index_col=false causes potential warning but can be ignored. Comment out two lines below to allow warnings again.
import warnings
warnings.filterwarnings('ignore')

import dask.dataframe as dd

# Print iterations progress credit to Greenstick from https://stackoverflow.com/questions/3173320/text-progress-bar-in-terminal-with-block-characters because I could not figure it out.
def printProgressBar (iteration, total, prefix = '', suffix = '', decimals = 1, length = 29, fill = '█', printEnd = "\r"):
	"""
	Call in a loop to create terminal progress bar
	@params:
		iteration   - Required  : current iteration (Int)
		total       - Required  : total iterations (Int)
		prefix      - Optional  : prefix string (Str)
		suffix      - Optional  : suffix string (Str)
		decimals    - Optional  : positive number of decimals in percent complete (Int)
		length      - Optional  : character length of bar (Int)
		fill        - Optional  : bar fill character (Str)
		printEnd    - Optional  : end character (e.g. "\r", "\r\n") (Str)
	"""
	percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
	filledLength = int(length * iteration // total)
	bar = fill * filledLength + '-' * (length - filledLength)
	print(f'\r{prefix} |{bar}| {percent}% {suffix}', end = printEnd)
	# Print New Line on Complete
	if iteration == total: 
		print()

printProgressBar(0, 29, prefix = 'Progress:                 ', suffix = 'Complete', length = 29)

# Importing CSV files in blocks to avoid overloading memory lower blocksize if needed. All data imported as string and then converted before being exported.
def importFiles():

	#change filepath variable to filepath to save game import/export file
	filepath = '/path/to/fhm11/saved_games/savegame.lg/import_export/csv/'

	dfPlayerMaster = dd.read_csv(filepath + 'player_master.csv', blocksize=100e6, sep = ';', on_bad_lines='skip', encoding='ISO-8859-15', header=0, names=['PlayerId', 'TeamId', 'FranchiseId', 'First_Name', 'Last_Name', 'Nick_Name', 'Height', 'Weight', 'DOB', 'Birthcity', 'Birthstate', 'Nationality_One', 'Nationality_Two', 'Nationality_Three', 'Retired', 'Hand', 'Rookie'], usecols=['PlayerId', 'TeamId', 'First_Name', 'Last_Name', 'Nick_Name', 'Height', 'Weight', 'DOB', 'Birthcity', 'Birthstate', 'Nationality_One', 'Nationality_Two', 'Nationality_Three', 'Retired', 'Hand', 'Rookie'], dtype='string')
	dfPlayerRatings = dd.read_csv(filepath + 'player_ratings.csv', blocksize=100e6, sep = ';', on_bad_lines='skip', encoding='ISO-8859-15', header=0, names=['PlayerId', 'Goalie', 'Left_D', 'Right_D', 'Left_W', 'Center', 'Right_W', 'Aggression', 'Bravery', 'Determination', 'Teamplayer', 'Leadership', 'Temperament', 'Professionalism', 'Goalie_Mental_Toughness', 'Goalie_Stamina', 'Acceleration', 'Agility', 'Balance', 'Speed', 'Stamina', 'Strength', 'Fighting', 'Screening', 'Getting_Open', 'Passing', 'Puck_Handling', 'Shooting_Accuracy', 'Shooting_Range', 'Offensive_Read', 'Checking', 'Faceoffs', 'Hitting', 'Positioning', 'Shot_Blocking', 'Stick_Checking', 'Defensive_Read', 'Goalie_Positioning', 'Goalie_Passing', 'Goalie_Poke_Checking', 'Goalie_Blocker', 'Goalie_Glove', 'Goalie_Rebound', 'Goalie_Recovery', 'Goalie_Puck_Handling', 'Goalie_Low_Shots', 'Goalie_Skating', 'Goalie_Reflexes', 'Skating', 'Shooting', 'Playmaking', 'Defending', 'Physicality', 'Conditioning', 'Character', 'Hockey_Sense', 'Goalie_Technique', 'Goalie_Overall_Positioning', 'Ability', 'Potential'], dtype='string')	
	dfPlayerRights = dd.read_csv(filepath + 'player_rights.csv', blocksize=100e6, sep = ';', on_bad_lines='skip', encoding='ISO-8859-15', header=0, names=['PlayerId', 'LeagueId', 'TeamId'], dtype='string')	
	dfPlayerContract = dd.read_csv(filepath + 'player_contract.csv', blocksize=100e6, sep = ';', on_bad_lines='skip', encoding='ISO-8859-15', header=0, names=['PlayerId', 'TeamId', 'NTC', 'NMC', 'ELC', 'UFA', 'Scholarship', 'Average_Salary', 'Major_Current_Year', 'Major_Next_Year', 'Major_3rd_Year', 'Major_4th_Year', 'Major_5th_Year', 'Major_6th_Year', 'Major_7th_Year', 'Major_8th_Year', 'Major_9th_Year', 'Major_10th_Year', 'Major_11th_Year', 'Major_12th_Year', 'Major_13th_Year', 'Major_14th_Year', 'Minor_Current_Year', 'Minor_Next_Year', 'Minor_3rd_Year', 'Minor_4th_Year', 'Minor_5th_Year', 'Minor_6th_Year', 'Minor_7th_Year', 'Minor_8th_Year', 'Minor_9th_Year', 'Minor_10th_Year', 'Minor_11th_Year', 'Minor_12th_Year', 'Minor_13th_Year', 'Minor_14th_Year'], dtype='string')		
	dfPlayerContractRenewed = dd.read_csv(filepath + 'player_contract_renewed.csv', blocksize=100e6, sep = ';', on_bad_lines='skip', encoding='ISO-8859-15', header=0, names=['PlayerId', 'TeamId', 'NTC', 'NMC', 'ELC', 'UFA', 'Scholarship', 'Average_Salary', 'Major_Next_Year', 'Major_3rd_Year', 'Major_4th_Year', 'Major_5th_Year', 'Major_6th_Year', 'Major_7th_Year', 'Major_8th_Year', 'Major_9th_Year', 'Major_10th_Year', 'Major_11th_Year', 'Major_12th_Year', 'Major_13th_Year', 'Major_14th_Year', 'Major_15th_Year', 'Minor_Next_Year', 'Minor_3rd_Year', 'Minor_4th_Year', 'Minor_5th_Year', 'Minor_6th_Year', 'Minor_7th_Year', 'Minor_8th_Year', 'Minor_9th_Year', 'Minor_10th_Year', 'Minor_11th_Year', 'Minor_12th_Year', 'Minor_13th_Year', 'Minor_14th_Year', 'Minor_15th_Year'], dtype='string')	
	dfBoxSkaterSummary = dd.read_csv(filepath + 'boxscore_skater_summary.csv', blocksize=100e6, sep = ';', on_bad_lines='skip', encoding='ISO-8859-15', header=0, names=['GameId', 'PlayerId', 'TeamId', 'GR', 'GR_Off', 'GR_Def', 'Goals', 'Assists', 'Plus_Minus', 'SOG', 'MS', 'BS', 'PIM', 'Hits', 'TK', 'GV', 'Shifts', 'TOI', 'PPTOI', 'SHTOI', 'EVTOI', 'FOW', 'FOL', 'FO_Per', 'Team_Shots_On', 'Team_SA_On', 'Team_Shots_Missed_On', 'Team_Shots_Missed_Against_On', 'Team_SB_On', 'Team_SB_Against_On', 'Team_Goals_On', 'Team_GA_On', 'Team_Shots_Off', 'Team_SA_Off', 'Team_Shots_Missed_Off', 'Team_Shots_Missed_Against_Off', 'Team_SB_Off', 'Team_SB_Against_Off', 'Team_Goals_Off', 'Team_GA_Off', 'OZ_Starts', 'NZ_Starts', 'DZ_Starts', 'Team_OZ_Starts', 'Team_NZ_Starts', 'Team_DZ_Starts', 'SQ0', 'SQ1', 'SQ2', 'SQ3', 'SQ4'], dtype='string')		
	dfPlayerSkaterRS = dd.read_csv(filepath + 'player_skater_stats_rs.csv', blocksize=100e6, sep = ';', on_bad_lines='skip', encoding='ISO-8859-15', header=0, names=['PlayerId', 'TeamId', 'FranchiseId', 'GP', 'Goals', 'Assists', 'Plus_Minus', 'PIM', 'PPG', 'PPA', 'SHG', 'SHA', 'Fights', 'Fights_Won', 'Hits', 'GV', 'TK', 'SB', 'GR', 'GR_Off', 'GR_Def', 'SOG', 'TOI', 'PPTOI', 'SHTOI', 'PDO', 'GF_60', 'GA_60', 'SF_60', 'SA_60', 'CF', 'CA', 'CF_Per', 'CF_Per_Rel', 'FF', 'FA', 'FF_Per', 'FF_Per_Rel', 'GWG', 'FO', 'FOW'], usecols=['PlayerId', 'TeamId', 'GP', 'Goals', 'Assists', 'Plus_Minus', 'PIM', 'PPG', 'PPA', 'SHG', 'SHA', 'Fights', 'Fights_Won', 'Hits', 'GV', 'TK', 'SB', 'GR', 'GR_Off', 'GR_Def', 'SOG', 'TOI', 'PPTOI', 'SHTOI', 'PDO', 'GF_60', 'GA_60', 'SF_60', 'SA_60', 'CF', 'CA', 'CF_Per', 'CF_Per_Rel', 'FF', 'FA', 'FF_Per', 'FF_Per_Rel', 'GWG', 'FO', 'FOW'],  dtype='string')
	dfPlayerSkaterPO = dd.read_csv(filepath + 'player_skater_stats_po.csv', blocksize=100e6, sep = ';', on_bad_lines='skip', encoding='ISO-8859-15', header=0, names=['PlayerId', 'TeamId', 'FranchiseId', 'GP', 'Goals', 'Assists', 'Plus_Minus', 'PIM', 'PPG', 'PPA', 'SHG', 'SHA', 'Fights', 'Fights_Won', 'Hits', 'GV', 'TK', 'SB', 'GR', 'GR_Off', 'GR_Def', 'SOG', 'TOI', 'PPTOI', 'SHTOI', 'PDO', 'GF_60', 'GA_60', 'SF_60', 'SA_60', 'CF', 'CA', 'CF_Per', 'CF_Per_Rel', 'FF', 'FA', 'FF_Per', 'FF_Per_Rel', 'GWG', 'FO', 'FOW'], usecols=['PlayerId', 'TeamId', 'GP', 'Goals', 'Assists', 'Plus_Minus', 'PIM', 'PPG', 'PPA', 'SHG', 'SHA', 'Fights', 'Fights_Won', 'Hits', 'GV', 'TK', 'SB', 'GR', 'GR_Off', 'GR_Def', 'SOG', 'TOI', 'PPTOI', 'SHTOI', 'PDO', 'GF_60', 'GA_60', 'SF_60', 'SA_60', 'CF', 'CA', 'CF_Per', 'CF_Per_Rel', 'FF', 'FA', 'FF_Per', 'FF_Per_Rel', 'GWG', 'FO', 'FOW'], dtype='string')
	dfPlayerSkaterPS = dd.read_csv(filepath + 'player_skater_stats_ps.csv', blocksize=100e6, sep = ';', on_bad_lines='skip', encoding='ISO-8859-15', header=0, names=['PlayerId', 'TeamId', 'FranchiseId', 'GP', 'Goals', 'Assists', 'Plus_Minus', 'PIM', 'PPG', 'PPA', 'SHG', 'SHA', 'Fights', 'Fights_Won', 'Hits', 'GV', 'TK', 'SB', 'GR', 'GR_Off', 'GR_Def', 'SOG', 'TOI', 'PPTOI', 'SHTOI', 'PDO', 'GF_60', 'GA_60', 'SF_60', 'SA_60', 'CF', 'CA', 'CF_Per', 'CF_Per_Rel', 'FF', 'FA', 'FF_Per', 'FF_Per_Rel', 'GWG', 'FO', 'FOW'], usecols=['PlayerId', 'TeamId', 'GP', 'Goals', 'Assists', 'Plus_Minus', 'PIM', 'PPG', 'PPA', 'SHG', 'SHA', 'Fights', 'Fights_Won', 'Hits', 'GV', 'TK', 'SB', 'GR', 'GR_Off', 'GR_Def', 'SOG', 'TOI', 'PPTOI', 'SHTOI', 'PDO', 'GF_60', 'GA_60', 'SF_60', 'SA_60', 'CF', 'CA', 'CF_Per', 'CF_Per_Rel', 'FF', 'FA', 'FF_Per', 'FF_Per_Rel', 'GWG', 'FO', 'FOW'], dtype='string')	
	dfBoxGoalieSummary = dd.read_csv(filepath + 'boxscore_goalie_summary.csv', blocksize=100e6, sep = ';', on_bad_lines='skip', encoding='ISO-8859-15', header=0, names=['GameId', 'PlayerId', 'TeamId', 'GR', 'SA', 'GA', 'SV', 'SV_Per', 'TOI', 'PIM'], dtype='string')		
	dfPlayerGoalieRS = dd.read_csv(filepath + 'player_goalie_stats_rs.csv', blocksize=100e6, sep = ';', on_bad_lines='skip', encoding='ISO-8859-15', header=0,names=['PlayerId', 'TeamId', 'FranchiseId', 'GP', 'G_Start', 'TOI', 'Wins', 'Losses', 'OT', 'SA', 'Saves', 'GA', 'GAA', 'Shutouts', 'SV_Per', 'GR'], usecols=['PlayerId', 'TeamId', 'GP', 'G_Start', 'TOI', 'Wins', 'Losses', 'OT', 'SA', 'Saves', 'GA', 'GAA', 'Shutouts', 'SV_Per', 'GR'], dtype='string')
	dfPlayerGoaliePO = dd.read_csv(filepath + 'player_goalie_stats_po.csv', blocksize=100e6, sep = ';', on_bad_lines='skip', encoding='ISO-8859-15', header=0,names=['PlayerId', 'TeamId', 'FranchiseId', 'GP', 'G_Start', 'TOI', 'Wins', 'Losses', 'OT', 'SA', 'Saves', 'GA', 'GAA', 'Shutouts', 'SV_Per', 'GR'], usecols=['PlayerId', 'TeamId', 'GP', 'G_Start', 'TOI', 'Wins', 'Losses', 'OT', 'SA', 'Saves', 'GA', 'GAA', 'Shutouts', 'SV_Per', 'GR'], dtype='string')
	dfPlayerGoaliePS = dd.read_csv(filepath + 'player_goalie_stats_ps.csv', blocksize=100e6, sep = ';', on_bad_lines='skip', encoding='ISO-8859-15', header=0,names=['PlayerId', 'TeamId', 'FranchiseId', 'GP', 'G_Start', 'TOI', 'Wins', 'Losses', 'OT', 'SA', 'Saves', 'GA', 'GAA', 'Shutouts', 'SV_Per', 'GR'], usecols=['PlayerId', 'TeamId', 'GP', 'G_Start', 'TOI', 'Wins', 'Losses', 'OT', 'SA', 'Saves', 'GA', 'GAA', 'Shutouts', 'SV_Per', 'GR'], dtype='string')	
	dfLeague = dd.read_csv(filepath + 'league_data.csv', blocksize=100e6, sep = ';', on_bad_lines='skip', encoding='ISO-8859-15', header=0, names=['LeagueId', 'League_Name', 'League_Abbr'], dtype='string')	
	dfConferences = dd.read_csv(filepath + 'conferences.csv', blocksize=100e6, sep = ';', on_bad_lines='skip', encoding='ISO-8859-15', header=0, names=['LeagueId', 'ConferenceId', 'Conference_Name'], dtype='string')
	dfDivisions = dd.read_csv(filepath + 'divisions.csv', blocksize=100e6, sep = ';', on_bad_lines='skip', encoding='ISO-8859-15', header=0, names=['LeagueId', 'ConferenceId', 'DivisionId', 'Division_Name'], usecols=['LeagueId', 'DivisionId', 'Division_Name'], dtype='string')
	dfSchedules = dd.read_csv(filepath + 'schedules.csv', blocksize=100e6, sep = ';', on_bad_lines='skip', encoding='ISO-8859-15', header=0, names=['LeagueId', 'Date', 'HomeId', 'Score_Home', 'AwayId', 'Score_Away', 'Type', 'Played', 'OT', 'SO', 'GameId'], dtype='string')	
	dfBoxGameSummary = dd.read_csv(filepath + 'boxscore_summary.csv', blocksize=100e6, sep = ';', on_bad_lines='skip', encoding='ISO-8859-15', header=0, names=['GameId', 'Date_Year', 'Date_Month', 'Date_Day', 'Arena', 'Attendance', 'HomeId', 'AwayId', 'Score_Home', 'Score_Away', 'Score_Home_P1', 'Score_Home_P2', 'Score_Home_P3', 'Score_Home_OT', 'Score_Home_SO', 'Score_Away_P1', 'Score_Away_P2', 'Score_Away_P3', 'Score_Away_OT', 'Score_Away_SO', 'Star1', 'Star2', 'Star3', 'Shots_Home', 'Shots_Away', 'PIM_Home', 'PIM_Away', 'Hits_Home', 'Hits_Away', 'GV_Home', 'GV_Away', 'TK_Home', 'TK_Away', 'FOW_Home', 'FOW_Away', 'SOG_Home_P1', 'SOG_Home_P2', 'SOG_Home_P3', 'SOG_Home_OT', 'SOG_Away_P1', 'SOG_Away_P2', 'SOG_Away_P3', 'SOG_Away_OT', 'PPG_Home', 'PPO_Home', 'PPG_Away', 'PPO_Away', 'Type', 'SQ0_Home', 'SQ1_Home', 'SQ2_Home', 'SQ3_Home', 'SQ4_Home', 'SQ0_Away', 'SQ1_Away', 'SQ2_Away', 'SQ3_Away', 'SQ4_Away'], dtype='string')
	dfBoxScoringSummary = dd.read_csv(filepath + 'boxscore_period_scoring_summary.csv', blocksize=100e6, sep = ';', on_bad_lines='skip', encoding='ISO-8859-15', header=0, names=['GameId', 'Score_Period', 'Score_Time', 'ScorerId', 'Assist1Id', 'Assist2Id', 'TeamId', 'Note', 'SQ'], dtype='string')
	dfBoxPenaltiesSummary = dd.read_csv(filepath + 'boxscore_period_penalties_summary.csv', blocksize=100e6, sep = ';', on_bad_lines='skip', encoding='ISO-8859-15', header=0, names=['GameId', 'Penalty_Period', 'Penalty_Time', 'PlayerId', 'TeamId', 'Penalty', 'Minutes'], dtype='string')
	dfTeamData = dd.read_csv(filepath + 'team_data.csv', blocksize=100e6, sep = ';', on_bad_lines='skip', encoding='ISO-8859-15', header=0, names=['TeamId', 'LeagueId', 'Team_Name', 'Team_Nickname', 'Team_Abbr', 'Parent_Team1', 'Parent_Team2', 'Parent_Team3', 'Parent_Team4', 'Parent_Team5', 'Parent_Team6', 'Parent_Team7', 'Parent_Team8', 'Primary_Colour', 'Secondary_Colour', 'Text_Colour', 'ConferenceId', 'DivisionId'], dtype='string')
	#team_lines.csv uses index_col=false because game file has 4 extra null values at end of each row
	dfTeamLines = dd.read_csv(filepath + 'team_lines.csv', blocksize=100e6, sep = ';', on_bad_lines='skip', encoding='ISO-8859-15', header=0, names=['TeamId', 'ES_L1_LW', 'ES_L1_C', 'ES_L1_RW', 'ES_L1_LD', 'ES_L1_RD', 'ES_L2_LW', 'ES_L2_C', 'ES_L2_RW', 'ES_L2_LD', 'ES_L2_RD', 'ES_L3_LW', 'ES_L3_C', 'ES_L3_RW', 'ES_L3_LD', 'ES_L3_RD', 'ES_L4_LW', 'ES_L4_C', 'ES_L4_RW', 'ES_L4_LD', 'ES_L4_RD', 'PP5on4_L1_LW', 'PP5on4_L1_C', 'PP5on4_L1_RW', 'PP5on4_L1_LD', 'PP5on4_L1_RD', 'PP5on4_L2_LW', 'PP5on4_L2_C', 'PP5on4_L2_RW', 'PP5on4_L2_LD', 'PP5on4_L2_RD', 'PP5on3_L1_LW', 'PP5on3_L1_C', 'PP5on3_L1_RW', 'PP5on3_L1_LD', 'PP5on3_L1_RD', 'PP5on3_L2_LW', 'PP5on3_L2_C', 'PP5on3_L2_RW', 'PP5on3_L2_LD', 'PP5on3_L2_RD', 'PP4on3_L1_F1', 'PP4on3_L1_F2', 'PP4on3_L1_LD', 'PP4on3_L1_RD', 'PP4on3_L2_F1', 'PP4on3_L2_F2', 'PP4on3_L2_LD', 'PP4on3_L2_RD', 'PK4on5_L1_F1', 'PK4on5_L1_F2', 'PK4on5_L1_LD', 'PK4on5_L1_RD', 'PK4on5_L2_F1', 'PK4on5_L2_F2', 'PK4on5_L2_LD', 'PK4on5 L2 RD', 'PK4on5_L3_F1', 'PK4on5_L3_F2', 'PK4on5_L3_LD', 'PK4on5_L3_RD', 'PK3on5_L1_F1', 'PK3on5_L1_LD', 'PK3on5_L1_RD', 'PK3on5_L2_F1', 'PK3on5_L2_LD', 'PK3on5_L2_RD', 'PK3on4_L1_F1', 'PK3on4_L1_LD', 'PK3on4_L1_RD', 'PK3on4_L2_F1', 'PK3on4_L2_LD', 'PK3on_L2_RD', '4on4_L1_F1', '4on4_L1_F2', '4on4_L1_LD', '4on4_L1_RD', '4on4_L2_F1', '4on4_L2_F2', '4on4_L2_LD', '4on4_L2_RD', '3on3_L1_F1', '3on3_L1_LD', '3on3_L1_RD', '3on3_L2_F1', '3on3_L2_LD', '3on3_L2_RD', 'Shootout1', 'Shootout2', 'Shootout3', 'Shootout4', 'Shootout5', 'Goalie1', 'Goalie2', 'Extra_Attacker1', 'Extra_Attacker2'], dtype='string', index_col = False)
	dfTeamStats = dd.read_csv(filepath + 'team_stats.csv', blocksize=100e6, sep = ';', on_bad_lines='skip', encoding='ISO-8859-15', header=0, names=['TeamId', 'GP', 'Goals', 'GA', 'Shots', 'SA', 'FO_Per', 'SB', 'Hits', 'TK', 'GV', 'Injury_Days', 'PIM_G', 'PP', 'PPG', 'SHGA', 'SH', 'PPGA', 'SHG', 'Att_Total_Home', 'Att_Total_Away', 'Att_Avg_Home', 'Att_Avg_Away', 'Sellouts_Home', 'Sellouts_Away', 'Capacity_Use_Per'], dtype='string')	
	dfTeamStatsPlayoffs = dd.read_csv(filepath + 'team_stats_playoffs.csv', blocksize=100e6, sep = ';', on_bad_lines='skip', encoding='ISO-8859-15', header=0, names=['TeamId', 'GP', 'Goals', 'GA', 'Shots', 'SA', 'FO_Per', 'SB', 'Hits', 'TK', 'GV', 'Injury_Days', 'PIM_G', 'PP', 'PPG', 'SHGA', 'SH', 'PPGA', 'SHG', 'Att_Total_Home', 'Att_Total_Away', 'Att_Avg_Home', 'Att_Avg_Away', 'Sellouts_Home', 'Sellouts_Away', 'Capacity_Use_Per'], dtype='string')	
	dfTeamRecords = dd.read_csv(filepath + 'team_records.csv', blocksize=100e6, sep = ';', on_bad_lines='skip', encoding='ISO-8859-15', header=0, names=['LeagueId', 'TeamId', 'ConfId', 'DivId', 'Wins', 'Losses', 'Ties', 'OTL', 'SO_Wins', 'SO_Losses', 'Points', 'GF', 'GA', 'PCT'], dtype='string')	
	dfDraftResult = dd.read_csv(filepath + 'draft_info.csv', blocksize=100e6, sep = ';', on_bad_lines='skip', encoding='ISO-8859-15', header=0, names=['PlayerId', 'DraftId', 'Year', 'Round', 'Pick', 'Overall', 'TeamId', 'TeamId_Picked_From'], dtype='string')
	dfDraftInfo = dd.read_csv(filepath + 'draft_index.csv', blocksize=100e6, sep = ';', on_bad_lines='skip', encoding='ISO-8859-15', header=0, names=['DraftId', 'Draft_Name'], dtype='string')
	dfStaffMaster = dd.read_csv(filepath + 'staff_master.csv', blocksize=100e6, sep = ';', on_bad_lines='skip', encoding='ISO-8859-15', header=0, names=['StaffId', 'TeamId', 'First_Name', 'Last_Name', 'Nick_Name', 'DOB', 'Birthcity', 'Birthstate', 'Nationality_One', 'Nationality_Two', 'Nationality_Three', 'Retired'], dtype='string')
	#staff_ratings.csv uses index_col=false because game file has ; at end of column headers on first line. Also use_cols to drop 'Blank' after naming third from the last column 'Blank' as there are too many columns.
	dfStaffRatings = dd.read_csv(filepath + 'staff_ratings.csv', blocksize=100e6, sep = ';', on_bad_lines='skip', encoding='ISO-8859-15', header=0, names=['StaffId', 'Executive', 'Manager', 'Coach', 'Scout', 'Trainer', 'Off_Pref', 'Phy_Pref', 'Line_Matching', 'Goalie_Handling', 'Favor_Veterans', 'Innovation', 'Loyalty', 'Coaching_G', 'Coaching_Defense', 'Coaching_Forwards', 'Coaching_Prospects', 'Def_Skills', 'Off_Skills', 'Phy_Training', 'Player_Management', 'Motivation', 'Discipline', 'Negotiating', 'Self-Preservation', 'Tactics', 'Ingame_Tactics', 'Trainer_Skill', 'Blank', 'Evaluate_Abilities', 'Evaluate_Potential'], usecols=['StaffId', 'Executive', 'Manager', 'Coach', 'Scout', 'Trainer', 'Off_Pref', 'Phy_Pref', 'Line_Matching', 'Goalie_Handling', 'Favor_Veterans', 'Innovation', 'Loyalty', 'Coaching_G', 'Coaching_Defense', 'Coaching_Forwards', 'Coaching_Prospects', 'Def_Skills', 'Off_Skills', 'Phy_Training', 'Player_Management', 'Motivation', 'Discipline', 'Negotiating', 'Self-Preservation', 'Tactics', 'Ingame_Tactics', 'Trainer_Skill', 'Evaluate_Abilities', 'Evaluate_Potential'], dtype='string', index_col=False)
 
	return([dfPlayerMaster, dfPlayerSkaterRS, dfTeamData, dfTeamLines, dfPlayerGoalieRS, dfPlayerContract, dfTeamStats, dfTeamRecords, dfSchedules, dfBoxSkaterSummary, dfBoxGoalieSummary, dfBoxGameSummary, dfBoxScoringSummary, dfBoxPenaltiesSummary, dfPlayerRatings, dfConferences, dfDivisions, dfLeague, dfDraftResult, dfDraftInfo, dfStaffMaster, dfStaffRatings, dfTeamStatsPlayoffs, dfPlayerContractRenewed, dfPlayerRights, dfPlayerSkaterPO, dfPlayerSkaterPS, dfPlayerGoaliePO, dfPlayerGoaliePS])

def getLeagues(dfSchedules, dfTeamData):
	#Get season start year and end year
	seasonStart =  dfSchedules['Date'].head(n=1, compute=True)
	seasonStart = seasonStart.values[0][0:4]
	seasonEnd = dfSchedules['Date'].tail(n=1, compute=True)
	seasonEnd = seasonEnd.values[0][0:4]
	season = seasonStart + '/' + seasonEnd

	#Define leagues that will be included in output files. LeagueIds found in league_data.csv
	leagues = ['0', '1', '2', '3', '4', '10', '11', '12', '13']

	teams = dfTeamData[['LeagueId', 'TeamId']].compute()
	teams = teams[teams['LeagueId'].isin(leagues)]['TeamId']
	teams = teams.astype('string')

	return(season, teams, leagues)

def simplifyFiles(files, season, teams, leagues):
	
	dfPlayerMaster, dfPlayerSkaterRS, dfTeamData, dfTeamLines, dfPlayerGoalieRS, dfPlayerContract, dfTeamStats, dfTeamRecords, dfSchedules, dfBoxSkaterSummary, dfBoxGoalieSummary, dfBoxGameSummary, dfBoxScoringSummary, dfBoxPenaltiesSummary, dfPlayerRatings, dfConferences, dfDivisions, dfLeague, dfDraftResult, dfDraftInfo, dfStaffMaster, dfStaffRatings, dfTeamStatsPlayoffs, dfPlayerContractRenewed, dfPlayerRights, dfPlayerSkaterPO, dfPlayerSkaterPS, dfPlayerGoaliePO, dfPlayerGoaliePS = files
	
	#Change outfilepath to desired folder. Currently default is simplifiedCSV in root folder.
	outfilepath = 'simplifiedCSV/'

	seasonvalue = season[0:4] + '-' + season[5:9]

	#Check conferences.csv, divisions.csv and league_data.csvs for correct number of non-null per row, check if the conferences are in the specified leagues, set datatypes before double merging into one dataframe, order columns and then export to league file.
	dfConferences = dfConferences[dfConferences.count(axis='columns') == 3]
	dfConferences = dfConferences[dfConferences['LeagueId'].isin(leagues)] 
	dfConferences = dfConferences.astype({'LeagueId': 'Int32', 'ConferenceId': 'Int32', 'Conference_Name': 'string'})

	dfDivisions = dfDivisions[dfDivisions.count(axis='columns') == 3]
	dfDivisions = dfDivisions[dfDivisions['LeagueId'].isin(leagues)]
	dfDivisions = dfDivisions.astype({'LeagueId': 'Int32', 'DivisionId': 'Int32', 'Division_Name': 'string'})

	dfLeague = dfLeague[dfLeague.count(axis='columns') == 3]
	dfLeague = dfLeague[dfLeague['LeagueId'].isin(leagues)]
	dfLeague = dfLeague.astype({'LeagueId': 'Int32', 'League_Name': 'string', 'League_Abbr': 'string'})

	dfConferenceDivisionSimplified = dd.merge(dfConferences, dfDivisions, on = 'LeagueId', how='left')
	dfConferenceDivisionLeagueSimplified = dd.merge(dfLeague, dfConferenceDivisionSimplified, on = 'LeagueId', how='left')

	dfConferenceDivisionLeagueSimplified = dfConferenceDivisionLeagueSimplified.assign(Season=season)
	dfConferenceDivisionLeagueSimplified = dfConferenceDivisionLeagueSimplified[['LeagueId', 'Season', 'ConferenceId', 'Conference_Name', 'DivisionId', 'Division_Name', 'League_Name', 'League_Abbr']]
	dfConferenceDivisionLeagueSimplified.to_parquet(outfilepath + 'league.parquet', name_function=lambda x: f'{seasonvalue}_league{x}.parquet', write_index=False)

	printProgressBar(3, 29, prefix = 'league.parquet:             ', suffix = 'Complete', length = 29)

	#Check player_master.csv for null values and fill the correct value in for each column, check if players have specified TeamId from specified LeagueId. Check player_ratings.csv for correct number of non-null per row. Then drop PlayerId with '-1' values as they are either duplicates or not needed. Set data type for both, merge into one dataframe, order columns and then export to players file. 

	dfPlayerMaster = dfPlayerMaster.fillna(value = {'PlayerId': '-1', 'TeamId': '-1', 'Season': '-1', 'First_Name': '-1', 'Last_Name': '-1', 'Nick_Name': 'None', 'Height': '-1', 'Weight': '-1', 'DOB': 'None', 'Birthcity': 'None', 'Birthstate': 'None', 'Nationality_One': 'None', 'Nationality_Two': 'None', 'Nationality_Three': 'None', 'Retired': '-1', 'Hand': 'None', 'Rookie': '-1'})
	dfPlayerMaster = dfPlayerMaster[dfPlayerMaster['PlayerId'].isin(['-1']) == False]
	dfPlayerMaster = dfPlayerMaster[dfPlayerMaster['TeamId'].isin(teams)]
	dfPlayerMaster = dfPlayerMaster.assign(Season=season)
	dfPlayerMaster = dfPlayerMaster.astype({'PlayerId': 'Int32', 'TeamId': 'Int32', 'Season': 'string', 'First_Name': 'string', 'Last_Name': 'string', 'Nick_Name': 'string', 'Height': 'Int32', 'Weight': 'Int32', 'DOB': 'string', 'Birthcity': 'string', 'Birthstate': 'string', 'Nationality_One': 'string', 'Nationality_Two': 'string', 'Nationality_Three': 'string', 'Retired': 'Int32', 'Hand': 'string', 'Rookie': 'Int32'})

	dfPlayerRatings = dfPlayerRatings[dfPlayerRatings.count(axis='columns') == 60]
	dfPlayerRatings = dfPlayerRatings.astype({'PlayerId': 'Int32', 'Goalie': 'Int32', 'Left_D': 'Int32', 'Right_D': 'Int32', 'Left_W': 'Int32', 'Center': 'Int32', 'Right_W': 'Int32', 'Aggression': 'Int32', 'Bravery': 'Int32', 'Determination': 'Int32', 'Teamplayer': 'Int32', 'Leadership': 'Int32', 'Temperament': 'Int32', 'Professionalism': 'Int32', 'Goalie_Mental_Toughness': 'Int32', 'Goalie_Stamina': 'Int32', 'Acceleration': 'Int32', 'Agility': 'Int32', 'Balance': 'Int32', 'Speed': 'Int32', 'Stamina': 'Int32', 'Strength': 'Int32', 'Fighting': 'Int32', 'Screening': 'Int32', 'Getting_Open': 'Int32', 'Passing': 'Int32', 'Puck_Handling': 'Int32', 'Shooting_Accuracy': 'Int32', 'Shooting_Range': 'Int32', 'Offensive_Read': 'Int32', 'Checking': 'Int32', 'Faceoffs': 'Int32', 'Hitting': 'Int32', 'Positioning': 'Int32', 'Shot_Blocking': 'Int32', 'Stick_Checking': 'Int32', 'Defensive_Read': 'Int32', 'Goalie_Positioning': 'Int32', 'Goalie_Passing': 'Int32', 'Goalie_Poke_Checking': 'Int32', 'Goalie_Blocker': 'Int32', 'Goalie_Glove': 'Int32', 'Goalie_Rebound': 'Int32', 'Goalie_Recovery': 'Int32', 'Goalie_Puck_Handling': 'Int32', 'Goalie_Low_Shots': 'Int32', 'Goalie_Skating': 'Int32', 'Goalie_Reflexes': 'Int32', 'Skating': 'Int32', 'Shooting': 'Int32', 'Playmaking': 'Int32', 'Defending': 'Int32', 'Physicality': 'Int32', 'Conditioning': 'Int32', 'Character': 'Int32', 'Hockey_Sense': 'Int32', 'Goalie_Technique': 'Int32', 'Goalie_Overall_Positioning': 'Int32', 'Ability': 'Float32', 'Potential': 'Float32'})

	dfPlayerMasterSimplified = dd.merge(dfPlayerMaster, dfPlayerRatings, on = 'PlayerId', how ='left')

	dfPlayerMasterSimplified = dfPlayerMasterSimplified[['PlayerId', 'TeamId', 'Season', 'First_Name', 'Last_Name', 'Nick_Name', 'Height', 'Weight', 'DOB', 'Hand', 'Goalie', 'Left_D', 'Right_D', 'Left_W', 'Center', 'Right_W', 'Screening', 'Getting_Open', 'Passing', 'Puck_Handling', 'Shooting_Accuracy', 'Shooting_Range', 'Offensive_Read', 'Checking', 'Faceoffs', 'Hitting', 'Positioning', 'Shot_Blocking', 'Stick_Checking', 'Defensive_Read', 'Acceleration', 'Agility', 'Balance', 'Speed', 'Stamina', 'Strength', 'Fighting', 'Goalie_Positioning', 'Goalie_Passing', 'Goalie_Poke_Checking', 'Goalie_Blocker', 'Goalie_Glove', 'Goalie_Rebound', 'Goalie_Recovery', 'Goalie_Puck_Handling', 'Goalie_Low_Shots', 'Goalie_Skating', 'Goalie_Reflexes', 'Goalie_Mental_Toughness', 'Goalie_Stamina', 'Aggression', 'Bravery', 'Determination', 'Teamplayer', 'Leadership', 'Temperament', 'Professionalism', 'Skating', 'Shooting', 'Playmaking', 'Defending', 'Physicality', 'Conditioning', 'Character', 'Hockey_Sense', 'Goalie_Technique', 'Goalie_Overall_Positioning', 'Ability', 'Potential']]

	dfPlayerMasterSimplified.to_parquet(outfilepath + 'players.parquet', name_function=lambda x: f'{seasonvalue}_players{x}.parquet', write_index=False)

	printProgressBar(5, 29, prefix = 'players.parquet:           ', suffix = 'Complete', length = 29)

	#Check player_skater_stats_rs.csv for correct number of non-null per row, check if players have specified TeamId from specified LeagueId. Set data types and order columns before exporting to skater_stats_rs file.
	dfPlayerSkaterRS = dfPlayerSkaterRS[dfPlayerSkaterRS.count(axis='columns') == 40]
	dfPlayerSkaterRS = dfPlayerSkaterRS[dfPlayerSkaterRS['TeamId'].isin(teams)]
	dfPlayerSkaterRS = dfPlayerSkaterRS.assign(Season=season)
	dfPlayerSkaterRS = dfPlayerSkaterRS.astype({'PlayerId': 'Int32', 'TeamId': 'Int32', 'Season': 'string', 'GP': 'Int32', 'Goals': 'Int32', 'Assists': 'Int32', 'Plus_Minus': 'Int32', 'PIM': 'Int32', 'PPG': 'Int32', 'PPA': 'Int32', 'SHG': 'Int32', 'SHA': 'Int32', 'Fights': 'Int32', 'Fights_Won': 'Int32', 'Hits': 'Int32', 'GV': 'Int32', 'TK': 'Int32', 'SB': 'Int32', 'GR': 'Int32', 'GR_Off': 'Int32', 'GR_Def': 'Int32', 'SOG': 'Int32', 'TOI': 'Int32', 'PPTOI': 'Int32', 'SHTOI': 'Int32', 'PDO': 'Float32', 'GF_60': 'Float32', 'GA_60': 'Float32', 'SF_60': 'Float32', 'SA_60': 'Float32', 'CF': 'Int32', 'CA': 'Int32', 'CF_Per': 'Float32', 'CF_Per_Rel': 'Float32', 'FF': 'Int32', 'FA': 'Int32', 'FF_Per': 'Float32', 'FF_Per_Rel': 'Float32', 'GWG': 'Int32', 'FO': 'Int32', 'FOW': 'Int32'})
	dfPlayerSkaterRS = dfPlayerSkaterRS[['PlayerId', 'TeamId', 'Season', 'GP', 'Goals', 'Assists', 'Plus_Minus', 'PIM', 'PPG', 'PPA', 'SHG', 'SHA', 'Fights', 'Fights_Won', 'Hits', 'GV', 'TK', 'SB', 'GR', 'GR_Off', 'GR_Def', 'SOG', 'TOI', 'PPTOI', 'SHTOI', 'PDO', 'GF_60', 'GA_60', 'SF_60', 'SA_60', 'CF', 'CA', 'CF_Per', 'CF_Per_Rel', 'FF', 'FA', 'FF_Per', 'FF_Per_Rel', 'GWG', 'FO', 'FOW']]

	dfPlayerSkaterRS.to_parquet(outfilepath + 'skater_stats_rs.parquet', name_function=lambda x: f'{seasonvalue}_skater_stats_rs{x}.parquet', write_index=False)

	printProgressBar(6, 29, prefix = 'skater_stats_rs.parquet:   ', suffix = 'Complete', length = 29)

	#Check player_skater_stats_po.csv for correct number of non-null per row, check if players have specified TeamId from specified LeagueId. Set data types and order columns before exporting to skater_stats_po file.
	dfPlayerSkaterPO = dfPlayerSkaterPO[dfPlayerSkaterPO.count(axis='columns') == 40]
	dfPlayerSkaterPO = dfPlayerSkaterPO[dfPlayerSkaterPO['TeamId'].isin(teams)]
	dfPlayerSkaterPO = dfPlayerSkaterPO.assign(Season=season)
	dfPlayerSkaterPO = dfPlayerSkaterPO.astype({'PlayerId': 'Int32', 'TeamId': 'Int32', 'Season': 'string', 'GP': 'Int32', 'Goals': 'Int32', 'Assists': 'Int32', 'Plus_Minus': 'Int32', 'PIM': 'Int32', 'PPG': 'Int32', 'PPA': 'Int32', 'SHG': 'Int32', 'SHA': 'Int32', 'Fights': 'Int32', 'Fights_Won': 'Int32', 'Hits': 'Int32', 'GV': 'Int32', 'TK': 'Int32', 'SB': 'Int32', 'GR': 'Int32', 'GR_Off': 'Int32', 'GR_Def': 'Int32', 'SOG': 'Int32', 'TOI': 'Int32', 'PPTOI': 'Int32', 'SHTOI': 'Int32', 'PDO': 'Float32', 'GF_60': 'Float32', 'GA_60': 'Float32', 'SF_60': 'Float32', 'SA_60': 'Float32', 'CF': 'Int32', 'CA': 'Int32', 'CF_Per': 'Float32', 'CF_Per_Rel': 'Float32', 'FF': 'Int32', 'FA': 'Int32', 'FF_Per': 'Float32', 'FF_Per_Rel': 'Float32', 'GWG': 'Int32', 'FO': 'Int32', 'FOW': 'Int32'})
	dfPlayerSkaterPO = dfPlayerSkaterPO[['PlayerId', 'TeamId', 'Season', 'GP', 'Goals', 'Assists', 'Plus_Minus', 'PIM', 'PPG', 'PPA', 'SHG', 'SHA', 'Fights', 'Fights_Won', 'Hits', 'GV', 'TK', 'SB', 'GR', 'GR_Off', 'GR_Def', 'SOG', 'TOI', 'PPTOI', 'SHTOI', 'PDO', 'GF_60', 'GA_60', 'SF_60', 'SA_60', 'CF', 'CA', 'CF_Per', 'CF_Per_Rel', 'FF', 'FA', 'FF_Per', 'FF_Per_Rel', 'GWG', 'FO', 'FOW']]

	dfPlayerSkaterPO.to_parquet(outfilepath + 'skater_stats_po.parquet', name_function=lambda x: f'{seasonvalue}_skater_stats_po{x}.parquet', write_index=False)

	printProgressBar(7, 29, prefix = 'skater_stats_po.parquet:   ', suffix = 'Complete', length = 29)

	#Check player_skater_stats_ps.csv for correct number of non-null per row, check if players have specified TeamId from specified LeagueId. Set data types and order columns before exporting to skater_stats_ps file.
	dfPlayerSkaterPS = dfPlayerSkaterPS[dfPlayerSkaterPS.count(axis='columns') == 40]
	dfPlayerSkaterPS = dfPlayerSkaterPS[dfPlayerSkaterPS['TeamId'].isin(teams)]
	dfPlayerSkaterPS = dfPlayerSkaterPS.assign(Season=season)
	dfPlayerSkaterPS = dfPlayerSkaterPS.astype({'PlayerId': 'Int32', 'TeamId': 'Int32', 'Season': 'string', 'GP': 'Int32', 'Goals': 'Int32', 'Assists': 'Int32', 'Plus_Minus': 'Int32', 'PIM': 'Int32', 'PPG': 'Int32', 'PPA': 'Int32', 'SHG': 'Int32', 'SHA': 'Int32', 'Fights': 'Int32', 'Fights_Won': 'Int32', 'Hits': 'Int32', 'GV': 'Int32', 'TK': 'Int32', 'SB': 'Int32', 'GR': 'Int32', 'GR_Off': 'Int32', 'GR_Def': 'Int32', 'SOG': 'Int32', 'TOI': 'Int32', 'PPTOI': 'Int32', 'SHTOI': 'Int32', 'PDO': 'Float32', 'GF_60': 'Float32', 'GA_60': 'Float32', 'SF_60': 'Float32', 'SA_60': 'Float32', 'CF': 'Int32', 'CA': 'Int32', 'CF_Per': 'Float32', 'CF_Per_Rel': 'Float32', 'FF': 'Int32', 'FA': 'Int32', 'FF_Per': 'Float32', 'FF_Per_Rel': 'Float32', 'GWG': 'Int32', 'FO': 'Int32', 'FOW': 'Int32'})
	dfPlayerSkaterPS = dfPlayerSkaterPS[['PlayerId', 'TeamId', 'Season', 'GP', 'Goals', 'Assists', 'Plus_Minus', 'PIM', 'PPG', 'PPA', 'SHG', 'SHA', 'Fights', 'Fights_Won', 'Hits', 'GV', 'TK', 'SB', 'GR', 'GR_Off', 'GR_Def', 'SOG', 'TOI', 'PPTOI', 'SHTOI', 'PDO', 'GF_60', 'GA_60', 'SF_60', 'SA_60', 'CF', 'CA', 'CF_Per', 'CF_Per_Rel', 'FF', 'FA', 'FF_Per', 'FF_Per_Rel', 'GWG', 'FO', 'FOW']]

	dfPlayerSkaterPS.to_parquet(outfilepath + 'skater_stats_ps.parquet', name_function=lambda x: f'{seasonvalue}_skater_stats_ps{x}.parquet', write_index=False)

	printProgressBar(8, 29, prefix = 'skater_stats_ps.parquet:   ', suffix = 'Complete', length = 29)

	#Check player_goalie_stats_rs.csv for correct number of non-null per row, check if players have specified TeamId from specified LeagueId. Convert SV_Per 00nan value to 0. Set data types and order columns before exporting to goalie_stats_rs file.

	dfPlayerGoalieRS = dfPlayerGoalieRS[dfPlayerGoalieRS.count(axis='columns') == 15]
	dfPlayerGoalieRS = dfPlayerGoalieRS[dfPlayerGoalieRS['TeamId'].isin(teams)]
	dfPlayerGoalieRS = dfPlayerGoalieRS.assign(Season=season)
	dfPlayerGoalieRS['SV_Per'] = dfPlayerGoalieRS['SV_Per'].replace('.00nan', '0')
	dfPlayerGoalieRS = dfPlayerGoalieRS.astype({'PlayerId': 'Int32', 'TeamId': 'Int32', 'Season': 'string', 'GP': 'Int32', 'TOI': 'Int32', 'TOI': 'Int32', 'Wins': 'Int32', 'Losses': 'Int32', 'OT': 'Int32', 'SA': 'Int32', 'Saves': 'Int32', 'GA': 'Int32', 'GAA': 'Float32', 'Shutouts': 'Int32', 'SV_Per': 'Float32', 'GR': 'Int32'})
	dfPlayerGoalieRS = dfPlayerGoalieRS[['PlayerId', 'TeamId', 'Season', 'GP', 'G_Start', 'TOI', 'Wins', 'Losses', 'OT', 'SA', 'Saves', 'GA', 'GAA', 'Shutouts', 'SV_Per', 'GR']]
	dfPlayerGoalieRS.to_parquet(outfilepath + 'goalie_stats_rs.parquet', name_function=lambda x: f'{seasonvalue}_goalie_stats_rs{x}.parquet', write_index=False)

	printProgressBar(9, 29, prefix = 'goalie_stats_rs.parquet:   ', suffix = 'Complete', length = 29)

	#Check player_goalie_stats_po.csv for correct number of non-null per row, check if players have specified TeamId from specified LeagueId. Convert SV_Per 00nan value to 0. Set data types and order columns before exporting to goalie_stats_po file.
	dfPlayerGoaliePO = dfPlayerGoaliePO[dfPlayerGoaliePO.count(axis='columns') == 15]
	dfPlayerGoaliePO = dfPlayerGoaliePO[dfPlayerGoaliePO['TeamId'].isin(teams)]
	dfPlayerGoaliePO = dfPlayerGoaliePO.assign(Season=season)
	dfPlayerGoaliePO['SV_Per'] = dfPlayerGoaliePO['SV_Per'].replace('.00nan', '0')
	dfPlayerGoaliePO = dfPlayerGoaliePO.astype({'PlayerId': 'Int32', 'TeamId': 'Int32', 'Season': 'string', 'GP': 'Int32', 'G_Start': 'Int32', 'TOI': 'Int32', 'Wins': 'Int32', 'Losses': 'Int32', 'OT': 'Int32', 'SA': 'Int32', 'Saves': 'Int32', 'GA': 'Int32', 'GAA': 'Float32', 'Shutouts': 'Int32', 'SV_Per': 'Float32', 'GR': 'Int32'})
	dfPlayerGoaliePO = dfPlayerGoaliePO[['PlayerId', 'TeamId', 'Season', 'GP', 'G_Start', 'TOI', 'Wins', 'Losses', 'OT', 'SA', 'Saves', 'GA', 'GAA', 'Shutouts', 'SV_Per', 'GR']]

	dfPlayerGoaliePO.to_parquet(outfilepath + 'goalie_stats_po.parquet', name_function=lambda x: f'{seasonvalue}_goalie_stats_po{x}.parquet', write_index=False)

	printProgressBar(10, 29, prefix = 'goalie_stats_po.parquet:   ', suffix = 'Complete', length = 29)

	#Check player_goalie_stats_ps.csv for correct number of non-null per row, check if players have specified TeamId from specified LeagueId. Convert SV_Per 00nan value to 0. Set data types and order columns before exporting to goalie_stats_ps file.
	dfPlayerGoaliePS = dfPlayerGoaliePS[dfPlayerGoaliePS.count(axis='columns') == 15]
	dfPlayerGoaliePS = dfPlayerGoaliePS[dfPlayerGoaliePS['TeamId'].isin(teams)]
	dfPlayerGoaliePS = dfPlayerGoaliePS.assign(Season=season)
	dfPlayerGoaliePS['SV_Per'] = dfPlayerGoaliePS['SV_Per'].replace('.00nan', '0')
	dfPlayerGoaliePS = dfPlayerGoaliePS.astype({'PlayerId': 'Int32', 'TeamId': 'Int32', 'Season': 'string', 'GP': 'Int32', 'G_Start': 'Int32', 'TOI': 'Int32', 'Wins': 'Int32', 'Losses': 'Int32', 'OT': 'Int32', 'SA': 'Int32', 'Saves': 'Int32', 'GA': 'Int32', 'GAA': 'Float32', 'Shutouts': 'Int32', 'SV_Per': 'Float32', 'GR': 'Int32'})
	dfPlayerGoaliePS = dfPlayerGoaliePS[['PlayerId', 'TeamId', 'Season', 'GP', 'G_Start', 'TOI', 'Wins', 'Losses', 'OT', 'SA', 'Saves', 'GA', 'GAA', 'Shutouts', 'SV_Per', 'GR']]

	dfPlayerGoaliePS.to_parquet(outfilepath + 'goalie_stats_ps.parquet', name_function=lambda x: f'{seasonvalue}_goalie_stats_ps{x}.parquet', write_index=False)

	printProgressBar(11, 29, prefix = 'goalie_stats_ps.parquet:   ', suffix = 'Complete', length = 29)

	#Check player_contract_renewed.csv for correct number of non-null per row, check if players have specified TeamId from specified LeagueId. Set data types and order columns before exporting to contract_renewed file.
	dfPlayerContractRenewed = dfPlayerContractRenewed[dfPlayerContractRenewed.count(axis='columns') == 36]
	dfPlayerContractRenewed = dfPlayerContractRenewed[dfPlayerContractRenewed['TeamId'].isin(teams)]
	dfPlayerContractRenewed = dfPlayerContractRenewed.assign(Season=season)
	dfPlayerContractRenewed = dfPlayerContractRenewed.astype({'PlayerId': 'Int32', 'TeamId': 'Int32', 'Season': 'string', 'NTC': 'string', 'NMC': 'string', 'ELC': 'string', 'UFA': 'string', 'Scholarship': 'string', 'Average_Salary': 'Int32', 'Major_Next_Year': 'Int32', 'Major_3rd_Year': 'Int32', 'Major_4th_Year': 'Int32', 'Major_5th_Year': 'Int32', 'Major_6th_Year': 'Int32', 'Major_7th_Year': 'Int32', 'Major_8th_Year': 'Int32', 'Major_9th_Year': 'Int32', 'Major_10th_Year': 'Int32', 'Major_11th_Year': 'Int32', 'Major_12th_Year': 'Int32', 'Major_13th_Year': 'Int32', 'Major_14th_Year': 'Int32', 'Major_15th_Year': 'Int32', 'Minor_Next_Year': 'Int32', 'Minor_3rd_Year': 'Int32', 'Minor_4th_Year': 'Int32', 'Minor_5th_Year': 'Int32', 'Minor_6th_Year': 'Int32', 'Minor_7th_Year': 'Int32', 'Minor_8th_Year': 'Int32', 'Minor_9th_Year': 'Int32', 'Minor_10th_Year': 'Int32', 'Minor_11th_Year': 'Int32', 'Minor_12th_Year': 'Int32', 'Minor_13th_Year': 'Int32', 'Minor_14th_Year': 'Int32', 'Minor_15th_Year': 'Int32'})
	dfPlayerContractRenewed = dfPlayerContractRenewed[['PlayerId', 'TeamId', 'Season', 'NTC', 'NMC', 'ELC', 'UFA', 'Scholarship', 'Average_Salary', 'Major_Next_Year', 'Major_3rd_Year', 'Major_4th_Year', 'Major_5th_Year', 'Major_6th_Year', 'Major_7th_Year', 'Major_8th_Year', 'Major_9th_Year', 'Major_10th_Year', 'Major_11th_Year', 'Major_12th_Year', 'Major_13th_Year', 'Major_14th_Year', 'Major_15th_Year', 'Minor_Next_Year', 'Minor_3rd_Year', 'Minor_4th_Year', 'Minor_5th_Year', 'Minor_6th_Year', 'Minor_7th_Year', 'Minor_8th_Year', 'Minor_9th_Year', 'Minor_10th_Year', 'Minor_11th_Year', 'Minor_12th_Year', 'Minor_13th_Year', 'Minor_14th_Year', 'Minor_15th_Year']]

	dfPlayerContractRenewed.to_parquet(outfilepath + 'contract_renewed.parquet', name_function=lambda x: f'{seasonvalue}_contract_renewed{x}.parquet', write_index=False)

	printProgressBar(12, 29, prefix = 'contract_renewed.parquet:  ', suffix = 'Complete', length = 29)

	#Check contract.csv for correct number of non-null per row, check if players have specified TeamId from specified LeagueId. Set data types and order columns before exporting to contract file.
	dfPlayerContract = dfPlayerContract[dfPlayerContract.count(axis='columns') == 36]
	dfPlayerContract = dfPlayerContract[dfPlayerContract['TeamId'].isin(teams)]
	dfPlayerContract = dfPlayerContract.assign(Season=season)
	dfPlayerContract = dfPlayerContract.astype({'PlayerId': 'Int32', 'TeamId': 'Int32', 'Season': 'string', 'NTC': 'string', 'NMC': 'string', 'ELC': 'string', 'UFA': 'string', 'Scholarship': 'string', 'Average_Salary': 'Int32', 'Major_Current_Year': 'Int32', 'Major_Next_Year': 'Int32', 'Major_3rd_Year': 'Int32', 'Major_4th_Year': 'Int32', 'Major_5th_Year': 'Int32', 'Major_6th_Year': 'Int32', 'Major_7th_Year': 'Int32', 'Major_8th_Year': 'Int32', 'Major_9th_Year': 'Int32', 'Major_10th_Year': 'Int32', 'Major_11th_Year': 'Int32', 'Major_12th_Year': 'Int32', 'Major_13th_Year': 'Int32', 'Major_14th_Year': 'Int32', 'Minor_Current_Year': 'Int32', 'Minor_Next_Year': 'Int32', 'Minor_3rd_Year': 'Int32', 'Minor_4th_Year': 'Int32', 'Minor_5th_Year': 'Int32', 'Minor_6th_Year': 'Int32', 'Minor_7th_Year': 'Int32', 'Minor_8th_Year': 'Int32', 'Minor_9th_Year': 'Int32', 'Minor_10th_Year': 'Int32', 'Minor_11th_Year': 'Int32', 'Minor_12th_Year': 'Int32', 'Minor_13th_Year': 'Int32', 'Minor_14th_Year': 'Int32'})
	dfPlayerContract = dfPlayerContract[['PlayerId', 'TeamId', 'Season', 'NTC', 'NMC', 'ELC', 'UFA', 'Scholarship', 'Average_Salary', 'Major_Current_Year', 'Major_Next_Year', 'Major_3rd_Year', 'Major_4th_Year', 'Major_5th_Year', 'Major_6th_Year', 'Major_7th_Year', 'Major_8th_Year', 'Major_9th_Year', 'Major_10th_Year', 'Major_11th_Year', 'Major_12th_Year', 'Major_13th_Year', 'Major_14th_Year', 'Minor_Current_Year', 'Minor_Next_Year', 'Minor_3rd_Year', 'Minor_4th_Year', 'Minor_5th_Year', 'Minor_6th_Year', 'Minor_7th_Year', 'Minor_8th_Year', 'Minor_9th_Year', 'Minor_10th_Year', 'Minor_11th_Year', 'Minor_12th_Year', 'Minor_13th_Year', 'Minor_14th_Year']]

	dfPlayerContract.to_parquet(outfilepath + 'contract.parquet', name_function=lambda x: f'{seasonvalue}_contract{x}.parquet', write_index=False)

	printProgressBar(13, 29, prefix = 'contract.parquet:          ', suffix = 'Complete', length = 29)

	#Check team_data.csv for correct number of non-null per row, check if teams have specified TeamId from specified LeagueId. Set data types and order columns before exporting to teams file.
	dfTeamData = dfTeamData[dfTeamData.count(axis='columns') == 18]
	dfTeamData = dfTeamData[dfTeamData['TeamId'].isin(teams)]
	dfTeamData = dfTeamData.assign(Season=season)
	dfTeamData = dfTeamData.astype({'TeamId': 'Int32', 'LeagueId': 'Int32', 'Team_Name': 'string', 'Team_Nickname': 'string', 'Team_Abbr': 'string', 'LeagueId': 'Int32', 'ConferenceId': 'Int32', 'DivisionId': 'Int32', 'Parent_Team1': 'string', 'Parent_Team2': 'string', 'Parent_Team3': 'string', 'Parent_Team4': 'string', 'Parent_Team5': 'string', 'Parent_Team6': 'string', 'Parent_Team7': 'string', 'Parent_Team8': 'string', 'Primary_Colour': 'string', 'Secondary_Colour': 'string', 'Text_Colour': 'string'})
	dfTeamData = dfTeamData[["TeamId", "Season", "Team_Name", "Team_Nickname", "Team_Abbr", "LeagueId", "ConferenceId", "DivisionId", "Parent_Team1", "Parent_Team2", "Parent_Team3", "Parent_Team4", "Parent_Team5", "Parent_Team6", "Parent_Team7", "Parent_Team8", "Primary_Colour", "Secondary_Colour", "Text_Colour"]]	

	dfTeamData.to_parquet(outfilepath + 'teams.parquet', name_function=lambda x: f'{seasonvalue}_teams{x}.parquet', write_index=False)

	printProgressBar(14, 29, prefix = 'teams.parquet:             ', suffix = 'Complete', length = 29)

	#Check team_lines.csv for null values and fill with value, check if players have specified TeamId from specified LeagueId. Set data types and order columns before exporting to team_lines file.
	dfTeamLines = dfTeamLines.fillna('-1')
	dfTeamLines = dfTeamLines[dfTeamLines['TeamId'].isin(teams)]
	dfTeamLines = dfTeamLines.astype('Int32')
	dfTeamLines = dfTeamLines.assign(Season=season)
	dfTeamLines = dfTeamLines[['TeamId', 'Season', 'ES_L1_LW', 'ES_L1_C', 'ES_L1_RW', 'ES_L1_LD', 'ES_L1_RD', 'ES_L2_LW', 'ES_L2_C', 'ES_L2_RW', 'ES_L2_LD', 'ES_L2_RD', 'ES_L3_LW', 'ES_L3_C', 'ES_L3_RW', 'ES_L3_LD', 'ES_L3_RD', 'ES_L4_LW', 'ES_L4_C', 'ES_L4_RW', 'ES_L4_LD', 'ES_L4_RD', 'PP5on4_L1_LW', 'PP5on4_L1_C', 'PP5on4_L1_RW', 'PP5on4_L1_LD', 'PP5on4_L1_RD', 'PP5on4_L2_LW', 'PP5on4_L2_C', 'PP5on4_L2_RW', 'PP5on4_L2_LD', 'PP5on4_L2_RD', 'PP5on3_L1_LW', 'PP5on3_L1_C', 'PP5on3_L1_RW', 'PP5on3_L1_LD', 'PP5on3_L1_RD', 'PP5on3_L2_LW', 'PP5on3_L2_C', 'PP5on3_L2_RW', 'PP5on3_L2_LD', 'PP5on3_L2_RD', 'PP4on3_L1_F1', 'PP4on3_L1_F2', 'PP4on3_L1_LD', 'PP4on3_L1_RD', 'PP4on3_L2_F1', 'PP4on3_L2_F2', 'PP4on3_L2_LD', 'PP4on3_L2_RD', 'PK4on5_L1_F1', 'PK4on5_L1_F2', 'PK4on5_L1_LD', 'PK4on5_L1_RD', 'PK4on5_L2_F1', 'PK4on5_L2_F2', 'PK4on5_L2_LD', 'PK4on5 L2 RD', 'PK4on5_L3_F1', 'PK4on5_L3_F2', 'PK4on5_L3_LD', 'PK4on5_L3_RD', 'PK3on5_L1_F1', 'PK3on5_L1_LD', 'PK3on5_L1_RD', 'PK3on5_L2_F1', 'PK3on5_L2_LD', 'PK3on5_L2_RD', 'PK3on4_L1_F1', 'PK3on4_L1_LD', 'PK3on4_L1_RD', 'PK3on4_L2_F1', 'PK3on4_L2_LD', 'PK3on_L2_RD', '4on4_L1_F1', '4on4_L1_F2', '4on4_L1_LD', '4on4_L1_RD', '4on4_L2_F1', '4on4_L2_F2', '4on4_L2_LD', '4on4_L2_RD', '3on3_L1_F1', '3on3_L1_LD', '3on3_L1_RD', '3on3_L2_F1', '3on3_L2_LD', '3on3_L2_RD', 'Shootout1', 'Shootout2', 'Shootout3', 'Shootout4', 'Shootout5', 'Goalie1', 'Goalie2', 'Extra_Attacker1', 'Extra_Attacker2']]

	dfTeamLines.to_parquet(outfilepath + 'team_lines.parquet', name_function=lambda x: f'{seasonvalue}_team_lines{x}.parquet', write_index=False)

	printProgressBar(15, 29, prefix = 'team_lines.parquet:        ', suffix = 'Complete', length = 29)

	#Check team_stats_playoffs.csv for correct number of non-null per row, check if players have specified TeamId from specified LeagueId. Set data types and order columns before exporting to team_stats_playoffs file.
	dfTeamStatsPlayoffs = dfTeamStatsPlayoffs[dfTeamStatsPlayoffs.count(axis='columns') == 26]
	dfTeamStatsPlayoffs = dfTeamStatsPlayoffs[dfTeamStatsPlayoffs['TeamId'].isin(teams)]
	dfTeamStatsPlayoffs = dfTeamStatsPlayoffs.astype({'TeamId': 'Int32', 'GP': 'Int32', 'Goals': 'Int32', 'GA': 'Int32', 'Shots': 'Int32', 'SA': 'Int32', 'FO_Per': 'Float32', 'SB': 'Int32', 'Hits': 'Int32', 'TK': 'Int32', 'GV': 'Int32', 'Injury_Days': 'Int32', 'PIM_G': 'Float32', 'PP': 'Int32', 'PPG': 'Int32', 'SHGA': 'Int32', 'SH': 'Int32', 'PPGA': 'Int32', 'SHG': 'Int32', 'Att_Total_Home': 'Int32', 'Att_Total_Away': 'Int32', 'Att_Avg_Home': 'Int32', 'Att_Avg_Away': 'Int32', 'Sellouts_Home': 'Int32', 'Sellouts_Away': 'Int32', 'Capacity_Use_Per': 'Int32'})
	dfTeamStatsPlayoffs = dfTeamStatsPlayoffs.assign(Season=season)
	dfTeamStatsPlayoffs = dfTeamStatsPlayoffs[['TeamId', 'Season', 'GP', 'Goals', 'GA', 'Shots', 'SA', 'SB', 'Hits', 'GV', 'TK', 'PIM_G', 'PP', 'PPG', 'PPGA', 'SH', 'SHG', 'SHGA', 'Injury_Days', 'Att_Total_Home', 'Att_Total_Away', 'Att_Avg_Home', 'Att_Avg_Away', 'Sellouts_Home', 'Sellouts_Away', 'Capacity_Use_Per']]

	dfTeamStatsPlayoffs.to_parquet(outfilepath + 'team_playoff_stats.parquet', name_function=lambda x: f'{seasonvalue}_team_playoff_stats{x}.parquet', write_index=False)

	printProgressBar(16, 29, prefix = 'team_playoff_stats,parquet:', suffix = 'Complete', length = 29)

	#Check team_stats.csv for correct number of non-null per row, check if players have specified TeamId from specified LeagueId. Check team_records.csv for correct number of non-null per row. Drop columns LeagueId, ConfId, DivId so when merging it does not cause duplicates. Set data type for both, merge into one dataframe, order columns and then export to team_stats file. 
	dfTeamStats = dfTeamStats[dfTeamStats.count(axis='columns') == 26]
	dfTeamStats = dfTeamStats.drop(columns = ['GA'])
	dfTeamStats = dfTeamStats[dfTeamStats['TeamId'].isin(teams)]
	dfTeamStats = dfTeamStats.astype({'TeamId': 'Int32', 'GP': 'Int32', 'Goals': 'Int32', 'Shots': 'Int32', 'SA': 'Int32', 'FO_Per': 'Float32', 'SB': 'Int32', 'Hits': 'Int32', 'TK': 'Int32', 'GV': 'Int32', 'Injury_Days': 'Int32', 'PIM_G': 'Float32', 'PP': 'Int32', 'PPG': 'Int32', 'SHGA': 'Int32', 'SH': 'Int32', 'PPGA': 'Int32', 'SHG': 'Int32', 'Att_Total_Home': 'Int32', 'Att_Total_Away': 'Int32', 'Att_Avg_Home': 'Int32', 'Att_Avg_Away': 'Int32', 'Sellouts_Home': 'Int32', 'Sellouts_Away': 'Int32', 'Capacity_Use_Per': 'Int32'})

	dfTeamRecords = dfTeamRecords[dfTeamRecords.count(axis='columns') == 14]
	dfTeamRecords = dfTeamRecords.drop(columns = ['LeagueId', 'ConfId', 'DivId'])
	dfTeamRecords = dfTeamRecords[dfTeamRecords['TeamId'].isin(teams)]
	dfTeamRecords = dfTeamRecords.astype({'TeamId': 'Int32', 'Wins': 'Int32', 'Losses': 'Int32', 'Ties': 'Int32', 'OTL': 'Int32', 'SO_Wins': 'Int32', 'SO_Losses': 'Int32', 'Points': 'Int32', 'GF': 'Int32', 'GA': 'Int32', 'PCT': 'Float32'})	
	dfTeamRecords = dfTeamRecords.assign(Season=season)

	dfTeamSimplified = dd.merge(dfTeamRecords, dfTeamStats, on = 'TeamId')

	dfTeamSimplified = dfTeamSimplified[['TeamId', 'Season', 'GP', 'Wins', 'Losses', 'Ties', 'OTL', 'SO_Wins', 'SO_Losses', 'Points', 'PCT', 'GF', 'GA', 'Shots', 'SA', 'SB', 'Hits', 'GV', 'TK', 'PIM_G', 'PP', 'PPG', 'PPGA', 'SH', 'SHG', 'SHGA', 'Injury_Days', 'Att_Total_Home', 'Att_Total_Away', 'Att_Avg_Home', 'Att_Avg_Away', 'Sellouts_Home', 'Sellouts_Away', 'Capacity_Use_Per']]

	dfTeamSimplified.to_parquet(outfilepath + 'team_stats.parquet', name_function=lambda x: f'{seasonvalue}_team_stats{x}.parquet', write_index=False)

	printProgressBar(18, 29, prefix = 'team_stats.parquet:        ', suffix = 'Complete', length = 29)

	#Check schedules.csv for correct number of non-null per row, check if schedule is for specified LeagueId. Check boxscore_summary.csv for correct number of non-null per row, check if AwayId is from specified TeamId from specified LeagueId. Drop columns AwayId, HomeId, Date_Year, Date_Month, Date_Day, Score_Home, Score_Away, Type so when merging it does not cause duplicates. Set data type for both, merge into one dataframe, order columns and then export to games_result file. 
	dfSchedules = dfSchedules[dfSchedules.count(axis='columns') == 11]
	dfSchedules = dfSchedules[dfSchedules['LeagueId'].isin(leagues)]
	dfSchedules = dfSchedules.astype({'LeagueId': 'Int32', 'Date': 'string', 'HomeId': 'Int32', 'Score_Home': 'Int32', 'AwayId': 'Int32', 'Score_Away': 'Int32', 'Type': 'string', 'Played': 'Int32', 'OT': 'Int32', 'SO': 'Int32', 'GameId': 'Int32'})

	dfBoxGameSummary = dfBoxGameSummary[dfBoxGameSummary.count(axis='columns') == 58]
	dfBoxGameSummary = dfBoxGameSummary[dfBoxGameSummary['AwayId'].isin(teams)]
	dfBoxGameSummary = dfBoxGameSummary.drop(columns = ['AwayId', 'HomeId', 'Date_Year', 'Date_Month', 'Date_Day', 'Score_Home', 'Score_Away', 'Type'])
	dfBoxGameSummary = dfBoxGameSummary.astype({'GameId': 'Int32', 'Arena': 'string', 'Attendance': 'Int32', 'Score_Home_P1': 'Int32', 'Score_Home_P2': 'Int32', 'Score_Home_P3': 'Int32', 'Score_Home_OT': 'Int32', 'Score_Home_SO': 'Int32', 'Score_Away_P1': 'Int32', 'Score_Away_P2': 'Int32', 'Score_Away_P3': 'Int32', 'Score_Away_OT': 'Int32', 'Score_Away_SO': 'Int32', 'Star1': 'Int32', 'Star2': 'Int32', 'Star3': 'Int32', 'Shots_Home': 'Int32', 'Shots_Away': 'Int32', 'PIM_Home': 'Int32', 'PIM_Away': 'Int32', 'Hits_Home': 'Int32', 'Hits_Away': 'Int32', 'GV_Home': 'Int32', 'GV_Away': 'Int32', 'TK_Home': 'Int32', 'TK_Away': 'Int32', 'FOW_Home': 'Int32', 'FOW_Away': 'Int32', 'SOG_Home_P1': 'Int32', 'SOG_Home_P2': 'Int32', 'SOG_Home_P3': 'Int32', 'SOG_Home_OT': 'Int32', 'SOG_Away_P1': 'Int32', 'SOG_Away_P2': 'Int32', 'SOG_Away_P3': 'Int32', 'SOG_Away_OT': 'Int32', 'PPG_Home': 'Int32', 'PPO_Home': 'Int32', 'PPG_Away': 'Int32', 'PPO_Away': 'Int32', 'SQ0_Home': 'Int32', 'SQ1_Home': 'Int32', 'SQ2_Home': 'Int32', 'SQ3_Home': 'Int32', 'SQ4_Home': 'Int32', 'SQ0_Away': 'Int32', 'SQ1_Away': 'Int32', 'SQ2_Away': 'Int32', 'SQ3_Away': 'Int32', 'SQ4_Away': 'Int32'})

	dfSchedulesGameSimplified = dd.merge(dfSchedules, dfBoxGameSummary, on = 'GameId', how = 'left')

	dfSchedulesGameSimplified = dfSchedulesGameSimplified[['LeagueId', 'Date', 'GameId', 'HomeId', 'Score_Home', 'AwayId', 'Score_Away', 'Type', 'Played', 'OT', 'SO', 'Arena', 'Attendance', 'Score_Home_P1', 'Score_Home_P2', 'Score_Home_P3', 'Score_Home_OT', 'Score_Home_SO', 'Score_Away_P1', 'Score_Away_P2', 'Score_Away_P3', 'Score_Away_OT', 'Score_Away_SO', 'Star1', 'Star2', 'Star3', 'Shots_Home', 'Shots_Away', 'PIM_Home', 'PIM_Away', 'Hits_Home', 'Hits_Away', 'GV_Home', 'GV_Away', 'TK_Home', 'TK_Away', 'FOW_Home', 'FOW_Away', 'SOG_Home_P1', 'SOG_Home_P2', 'SOG_Home_P3', 'SOG_Home_OT', 'SOG_Away_P1', 'SOG_Away_P2', 'SOG_Away_P3', 'SOG_Away_OT', 'PPG_Home', 'PPG_Away', 'PPO_Home', 'PPO_Away', 'SQ0_Home', 'SQ1_Home', 'SQ2_Home', 'SQ3_Home', 'SQ4_Home', 'SQ0_Away', 'SQ1_Away', 'SQ2_Away', 'SQ3_Away', 'SQ4_Away']]

	dfSchedulesGameSimplified.to_parquet(outfilepath + 'games_result.parquet', name_function=lambda x: f'{seasonvalue}_games_result{x}.parquet', write_index=False)

	printProgressBar(20, 29, prefix = 'games_result.parquet:      ', suffix = 'Complete', length = 29)

	#Check boxscore_period_scoring_summary.csv for correct number of non-null per row, check if TeamId have specified TeamId from specified LeagueId. Set data types and order columns before exporting to games_scores file.
	dfBoxScoringSummary = dfBoxScoringSummary[dfBoxScoringSummary.count(axis='columns') == 9]
	dfBoxScoringSummary = dfBoxScoringSummary[dfBoxScoringSummary['TeamId'].isin(teams)]
	dfBoxScoringSummary = dfBoxScoringSummary.astype({'GameId': 'Int32', 'Score_Period': 'string', 'Score_Time': 'Int32', 'ScorerId': 'Int32', 'Assist1Id': 'Int32', 'Assist2Id': 'Int32', 'TeamId': 'Int32', 'Note': 'string', 'SQ': 'Int32'})
	dfBoxScoringSummary = dfBoxScoringSummary[['GameId', 'Score_Period', 'Score_Time', 'ScorerId', 'Assist1Id', 'Assist2Id', 'TeamId', 'Note', 'SQ']]

	dfBoxScoringSummary.to_parquet(outfilepath + 'games_scores.parquet', name_function=lambda x: f'{seasonvalue}_games_scores{x}.parquet', write_index=False)

	printProgressBar(21, 29, prefix = 'games_scores.parquet:      ', suffix = 'Complete', length = 29)

	#Check boxscore_period_penalties_summary.csv for correct number of non-null per row, check if TeamId have specified TeamId from specified LeagueId. Set data types and order columns before exporting to games_penalties file.
	dfBoxPenaltiesSummary = dfBoxPenaltiesSummary[dfBoxPenaltiesSummary.count(axis='columns') == 7]
	dfBoxPenaltiesSummary = dfBoxPenaltiesSummary[dfBoxPenaltiesSummary['TeamId'].isin(teams)]
	dfBoxPenaltiesSummary = dfBoxPenaltiesSummary.astype({'GameId': 'Int32', 'Penalty_Period': 'string', 'Penalty_Time': 'Int32', 'PlayerId': 'Int32', 'TeamId': 'Int32', 'Penalty': 'string', 'Minutes': 'Int32'})
	dfBoxPenaltiesSummary = dfBoxPenaltiesSummary[['GameId', 'Penalty_Period', 'Penalty_Time', 'PlayerId', 'TeamId', 'Penalty', 'Minutes']]

	dfBoxPenaltiesSummary.to_parquet(outfilepath + 'games_penalties.parquet', name_function=lambda x: f'{seasonvalue}_games_penalties{x}.parquet', write_index=False)

	printProgressBar(22, 29, prefix = 'games_penalties.parquet:   ', suffix = 'Complete', length = 29)

	#Check boxscore_goalie_summary.csv for correct number of non-null per row, check if TeamId have specified TeamId from specified LeagueId. Convert SV_Per 00nan value to 0. Set data types, merge with dfSchedules, which was previously sanitized and filter out rows where Played column is not 0, order columns before exporting to goalie_stats_game file.
	dfBoxGoalieSummary = dfBoxGoalieSummary[dfBoxGoalieSummary.count(axis='columns') == 10]
	dfBoxGoalieSummary = dfBoxGoalieSummary[dfBoxGoalieSummary['TeamId'].isin(teams)]
	dfBoxGoalieSummary = dfBoxGoalieSummary.assign(Season=season)
	dfBoxGoalieSummary['SV_Per'] = dfBoxGoalieSummary['SV_Per'].replace('.00nan', '0')
	dfBoxGoalieSummary = dfBoxGoalieSummary.astype({'GameId': 'Int32', 'PlayerId': 'Int32', 'TeamId': 'Int32', 'GR': 'Float32', 'SA': 'Int32', 'GA': 'Int32', 'SV': 'Int32', 'SV_Per': 'float32', 'TOI': 'Int32', 'PIM': 'Int32'})

	dfGameIdGoalie = dd.merge(dfSchedules, dfBoxGoalieSummary, on='GameId', how='left')

	dfGameIdGoalie = dfGameIdGoalie[dfGameIdGoalie['Played'].isin([0]) == False]
	dfGameIdGoalie = dfGameIdGoalie.drop(columns = ['Played', 'HomeId', 'Score_Home', 'AwayId', 'Score_Away', 'OT', 'SO'])
	dfGameIdGoalie = dfGameIdGoalie[['LeagueId', 'Date', 'Type', 'GameId', 'PlayerId', 'TeamId', 'GR', 'SA', 'GA', 'SV', 'SV_Per', 'TOI', 'PIM']]

	dfGameIdGoalie.to_parquet(outfilepath + 'goalie_stats_game.parquet', name_function=lambda x: f'{seasonvalue}_goalie_stats_game{x}.parquet', write_index=False)

	printProgressBar(23, 29, prefix = 'goalie_stats_game.parquet: ', suffix = 'Complete', length = 29)

	#Check boxscore_skater_summary.csv for correct number of non-null per row, check if TeamId have specified TeamId from specified LeagueId. Set data types, merge with dfSchedules, which was previously sanitized and filter out rows where Played column is not 0, order columns before exporting to skater_stats_game file.
	dfBoxSkaterSummary = dfBoxSkaterSummary[dfBoxSkaterSummary.count(axis='columns') == 51]
	dfBoxSkaterSummary = dfBoxSkaterSummary[dfBoxSkaterSummary['TeamId'].isin(teams)]
	dfBoxSkaterSummary = dfBoxSkaterSummary.assign(Season=season)
	dfBoxSkaterSummary = dfBoxSkaterSummary.astype({'GameId': 'Int32', 'PlayerId': 'Int32', 'TeamId': 'Int32', 'GR': 'Float32', 'GR_Off': 'Float32', 'GR_Def': 'Float32', 'Goals': 'Int32', 'Assists': 'Int32', 'Plus_Minus': 'Int32', 'SOG': 'Int32', 'MS': 'Int32', 'BS': 'Int32', 'PIM': 'Int32', 'Hits': 'Int32', 'TK': 'Int32', 'GV': 'Int32', 'Shifts': 'Int32', 'TOI': 'Int32', 'PPTOI': 'Int32', 'SHTOI': 'Int32', 'EVTOI': 'Int32', 'FOW': 'Int32', 'FOL': 'Int32', 'FO_Per': 'Float32', 'Team_Shots_On': 'Int32', 'Team_SA_On': 'Int32', 'Team_Shots_Missed_On': 'Int32', 'Team_Shots_Missed_Against_On': 'Int32', 'Team_SB_On': 'Int32', 'Team_SB_Against_On': 'Int32', 'Team_Goals_On': 'Int32', 'Team_GA_On': 'Int32', 'Team_Shots_Off': 'Int32', 'Team_SA_Off': 'Int32', 'Team_Shots_Missed_Off': 'Int32', 'Team_Shots_Missed_Against_Off': 'Int32', 'Team_SB_Off': 'Int32', 'Team_SB_Against_Off': 'Int32', 'Team_Goals_Off': 'Int32', 'Team_GA_Off': 'Int32', 'OZ_Starts': 'Int32', 'NZ_Starts': 'Int32', 'DZ_Starts': 'Int32', 'Team_OZ_Starts': 'Int32', 'Team_NZ_Starts': 'Int32', 'Team_DZ_Starts': 'Int32', 'SQ0': 'Int32', 'SQ1': 'Int32', 'SQ2': 'Int32', 'SQ3': 'Int32', 'SQ4': 'Int32'})

	dfGameIdPlayer = dd.merge(dfSchedules, dfBoxSkaterSummary, on='GameId', how='left')

	dfGameIdPlayer = dfGameIdPlayer[dfGameIdPlayer['Played'].isin([0]) == False]
	dfGameIdPlayer = dfGameIdPlayer.drop(columns = ['Played', 'HomeId', 'Score_Home', 'AwayId', 'Score_Away', 'OT', 'SO'])
	dfGameIdPlayer = dfGameIdPlayer[['LeagueId', 'Date', 'Type', 'GameId', 'PlayerId', 'TeamId', 'GR', 'GR_Off', 'GR_Def', 'Goals', 'Assists', 'Plus_Minus', 'SOG', 'MS', 'BS', 'PIM', 'Hits', 'GV', 'TK', 'Shifts', 'TOI', 'PPTOI', 'SHTOI', 'EVTOI', 'FOW', 'FOL', 'FO_Per', 'Team_Shots_On', 'Team_Shots_Off', 'Team_Shots_Missed_On', 'Team_Shots_Missed_Off', 'Team_SB_On', 'Team_SB_Off', 'Team_Goals_On', 'Team_Goals_Off', 'Team_SA_On', 'Team_SA_Off', 'Team_SB_Against_On', 'Team_SB_Against_Off', 'Team_GA_On', 'Team_GA_Off', 'Team_Shots_Missed_Against_On', 'Team_Shots_Missed_Against_Off', 'OZ_Starts', 'NZ_Starts', 'DZ_Starts', 'Team_OZ_Starts', 'Team_NZ_Starts', 'Team_DZ_Starts', 'SQ0', 'SQ1', 'SQ2', 'SQ3', 'SQ4']]

	dfGameIdPlayer.to_parquet(outfilepath + 'skater_stats_game.parquet', name_function=lambda x: f'{seasonvalue}_skater_stats_game{x}.parquet', write_index=False)

	printProgressBar(24, 29, prefix = 'skater_stats_game.parquet: ', suffix = 'Complete', length = 29)

	#Check staff_master.csv for null values and fill the correct value in for each column. Then keep Retired with value 0 as they are not retired. Check staff_ratings.csv for null values and fill the correct value in for each column. Then drop StaffId with '-1' values as they are either duplicates or not needed. Drop duplicates as the files contain ~700 duplicate StaffIds at end of CSV. Set data type for both, merge into one dataframe, order columns and then export to staff file. 
	dfStaffMaster = dfStaffMaster.fillna(value = {'StaffId': '-1', 'TeamId': '-1', 'Season': '-1', 'First_Name': '-1', 'Last_Name': '-1', 'Nick_Name': 'None', 'DOB': 'None', 'Birthcity': 'None', 'Birthstate': 'None', 'Nationality_One': 'None', 'Nationality_Two': 'None', 'Nationality_Three': 'None', 'Retired': '-1'})
	dfStaffMaster = dfStaffMaster[dfStaffMaster['StaffId'].isin(['-1']) == False]
	dfStaffMaster = dfStaffMaster[dfStaffMaster['Retired'].isin(['0']) == True]
	dfStaffMaster = dfStaffMaster.drop_duplicates(subset=['StaffId'])
	dfStaffMaster = dfStaffMaster.assign(Season=season)
	dfStaffMaster = dfStaffMaster.astype({'StaffId': 'Int32', 'TeamId': 'Int32', 'Season': 'string', 'First_Name': 'string', 'Last_Name': 'string', 'Nick_Name': 'string', 'DOB': 'string', 'Birthcity': 'string', 'Birthstate': 'string', 'Nationality_One': 'string', 'Nationality_Two': 'string', 'Nationality_Three': 'string', 'Retired': 'Int32'})

	dfStaffRatings = dfStaffRatings.fillna(value = {'StaffId': '-1', 'Executive': '-1', 'Manager': '-1', 'Coach': '-1', 'Scout': '-1', 'Trainer': '-1', 'Off_Pref': 'Defensive', 'Phy_Pref': 'Non-Physical', 'Line_Matching': 'Balanced', 'Goalie_Handling': 'Balanced', 'Favor_Veterans': 'Balanced', 'Innovation': 'Balanced', 'Loyalty': 'Balanced', 'Coaching_G': '-1', 'Coaching_Defense': '-1', 'Coaching_Forwards': '-1', 'Coaching_Prospects': '-1', 'Def_Skills': '-1', 'Off_Skills': '-1', 'Phy_Training': '-1', 'Player_Management': '-1', 'Motivation': '-1', 'Discipline': '-1', 'Negotiating': '-1', 'Self-Preservation': '-1', 'Tactics': '-1', 'Ingame_Tactics': '-1', 'Trainer_Skill': '-1', 'Evaluate_Abilities': '-1', 'Evaluate_Potential': '-1'})
	dfStaffRatings = dfStaffRatings[dfStaffRatings['StaffId'].isin(['-1']) == False]
	dfStaffRatings = dfStaffRatings.drop_duplicates(subset=['StaffId'])
	dfStaffRatings = dfStaffRatings.astype({'StaffId': 'Int32', 'Executive': 'Int32', 'Manager': 'Int32', 'Coach': 'Int32', 'Scout': 'Int32', 'Trainer': 'Int32', 'Off_Pref': 'string', 'Phy_Pref': 'string', 'Line_Matching': 'string', 'Goalie_Handling': 'string', 'Favor_Veterans': 'string', 'Innovation': 'string', 'Loyalty': 'string', 'Coaching_G': 'Int32', 'Coaching_Defense': 'Int32', 'Coaching_Forwards': 'Int32', 'Coaching_Prospects': 'Int32', 'Def_Skills': 'Int32', 'Off_Skills': 'Int32', 'Phy_Training': 'Int32', 'Player_Management': 'Int32', 'Motivation': 'Int32', 'Discipline': 'Int32', 'Negotiating': 'Int32', 'Self-Preservation': 'Int32', 'Tactics': 'Int32', 'Ingame_Tactics': 'Int32', 'Trainer_Skill': 'Int32', 'Evaluate_Abilities': 'Int32', 'Evaluate_Potential': 'Int32'})

	dfStaff = dd.merge(dfStaffMaster, dfStaffRatings, on = 'StaffId', how='left')

	dfStaff = dfStaff[['StaffId', 'TeamId', 'Season', 'First_Name', 'Last_Name', 'Nick_Name', 'DOB', 'Birthcity', 'Birthstate', 'Nationality_One', 'Nationality_Two', 'Nationality_Three', 'Retired', 'Executive', 'Manager', 'Coach', 'Scout', 'Trainer', 'Off_Pref', 'Phy_Pref', 'Line_Matching', 'Goalie_Handling', 'Favor_Veterans', 'Innovation', 'Loyalty', 'Coaching_G', 'Coaching_Defense', 'Coaching_Forwards', 'Coaching_Prospects', 'Def_Skills', 'Off_Skills', 'Phy_Training', 'Player_Management', 'Motivation', 'Discipline', 'Negotiating', 'Self-Preservation', 'Tactics', 'Ingame_Tactics', 'Trainer_Skill', 'Evaluate_Abilities', 'Evaluate_Potential']]

	dfStaff.to_parquet(outfilepath + 'staff.parquet', name_function=lambda x: f'{seasonvalue}_staff{x}.parquet', write_index=False)

	printProgressBar(26, 29, prefix = 'staff.parquet:             ', suffix = 'Complete', length = 29)

	#Check draft_info.csv for correct number of non-null per row. Check draft_index.csv for correct number of non-null per row. Set data type for both, merge into one dataframe. Add +1 to dfDraft Year and turn into Season and add back to dataframe. Order columns and then export to draft file. 
	dfDraftResult = dfDraftResult[dfDraftResult.count(axis='columns') == 8]
	dfDraftResult = dfDraftResult.astype({'PlayerId': 'Int32', 'DraftId': 'Int32', 'Year': 'Int32', 'Round': 'Int32', 'Pick': 'Int32', 'Overall': 'Int32', 'TeamId': 'Int32', 'TeamId_Picked_From': 'Int32'})
	
	dfDraftInfo = dfDraftInfo[dfDraftInfo.count(axis='columns') == 2]
	dfDraftInfo = dfDraftInfo.astype({'DraftId': 'Int32', 'Draft_Name': 'string'})

	dfDraft = dd.merge(dfDraftResult, dfDraftInfo, on = 'DraftId', how='left')

	dfDraft1 = dfDraft['Year'].astype(int)
	dfDraft2 = dfDraft['Year'].astype(int) + 1

	dfDraft['Season'] = dfDraft1.astype(str) + '/' + dfDraft2.astype(str)

	dfDraft = dfDraft[['PlayerId', 'DraftId', 'Season', 'Round', 'Pick', 'Overall', 'TeamId', 'TeamId_Picked_From', 'Draft_Name']]

	dfDraft.to_parquet(outfilepath + 'draft.parquet', name_function=lambda x: f'{seasonvalue}_draft{x}.parquet', write_index=False)

	printProgressBar(28, 29, prefix = 'draft.parquet:             ', suffix = 'Complete', length = 29)

	#Check player_rights.csv for correct number of non-null per row, check if TeamId have specified TeamId from specified LeagueId. Set data types and order columns before exporting to player_rights file.
	dfPlayerRights = dfPlayerRights[dfPlayerRights.count(axis='columns') == 3]
	dfPlayerRights = dfPlayerRights[dfPlayerRights['TeamId'].isin(teams)]
	dfPlayerRights = dfPlayerRights.assign(Season=season)
	dfPlayerRights = dfPlayerRights.astype({'Season': 'string', 'PlayerId': 'Int32', 'LeagueId': 'Int32', 'TeamId': 'Int32'})
	dfPlayerRights = dfPlayerRights[['PlayerId', 'Season', 'LeagueId', 'TeamId']]

	dfPlayerRights.to_parquet(outfilepath + 'player_rights.parquet', name_function=lambda x: f'{seasonvalue}_player_rights{x}.parquet', write_index=False)

	printProgressBar(29, 29, prefix = 'player_rights.parquet:     ', suffix = 'Complete', length = 29)

def main():
	files = importFiles()
	season, teams, leagues = getLeagues(files[8], files[2])
	simplifyFiles(files, season, teams, leagues)

if __name__ == '__main__':
	main()
