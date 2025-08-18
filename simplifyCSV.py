#Files exported as both csv and parquet format.
#Leagues to filter through and blocksize for dask partitions. Hard coded on line 61 and in each file structure in config.json. Import folder file path hard coded on line 36. Export folder file path hard coded on line 73. Will later be defined in separate file.

#pip install dask[dataframe]
import dask.dataframe as dd
#built in python packages
import json
from io import StringIO

# Print iterations progress credit to Greenstick from https://stackoverflow.com/questions/3173320/text-progress-bar-in-terminal-with-block-characters because I could not figure it out.
def printProgressBar (iteration, total, prefix = '', suffix = '', decimals = 1, length = 21, fill = '█', printEnd = "\r\n"):
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
	print(f'\r{percent}% {prefix} {suffix}', end = printEnd)
	# Print New Line on Complete
	if iteration == total: 
		print()

# Importing CSV files in blocks to avoid overloading memory lower blocksize if needed. All data imported as string and then converted before being exported.
def importFiles():

 	#change filepath variable to filepath to save game import/export file
 	filepath = 'csv/import_export/'

 	dfSchedules = dd.read_csv(filepath + 'schedules.csv', blocksize=100e6, sep = ';', on_bad_lines='skip', encoding='ISO-8859-15', header=0, names=['LeagueId', 'Dates', 'HomeId', 'Score_Home', 'AwayId', 'Score_Away', 'Types', 'Played', 'OT', 'SO', 'GameId'], dtype='string')	
 	dfTeamData = dd.read_csv(filepath + 'team_data.csv', blocksize=100e6, sep = ';', on_bad_lines='skip', encoding='ISO-8859-15', header=0, names=['TeamId', 'LeagueId', 'Team_Name', 'Team_Nickname', 'Team_Abbr', 'Parent_Team1', 'Parent_Team2', 'Parent_Team3', 'Parent_Team4', 'Parent_Team5', 'Parent_Team6', 'Parent_Team7', 'Parent_Team8', 'Primary_Colour', 'Secondary_Colour', 'Text_Colour', 'ConferenceId', 'DivisionId'], dtype='string')
 
 	return([dfTeamData, dfSchedules])

def getLeagues(dfTeamData, dfSchedules):
	#Get season start year and end year
	seasonStart =  dfSchedules['Dates'].head(n=1, compute=True)
	seasonStart = seasonStart.values[0][0:4]
	seasonEnd = dfSchedules['Dates'].tail(n=1, compute=True)
	seasonEnd = seasonEnd.values[0][0:4]
	season = seasonStart + '/' + seasonEnd

	#Using schedules.csv find last played game and store Dates to add to team lines to find approximate lines at date.
	dfSchedules = dfSchedules[dfSchedules['Played'].isin(['0'])].head(n=1, compute=True).reset_index()

	#If all games played default to end of Season.
	if dfSchedules.empty:
		exportDate = '2025-06-30'
	else:
		exportDate = dfSchedules['Dates'][0]


	#Define leagues that will be included in output files. LeagueIds found in league_data.csv
	leagues = ['0', '1', '2', '3', '4', '10', '11', '12', '13', '14']

	#Using leagues find TeamIds in those leagues.
	teams = dfTeamData[['LeagueId', 'TeamId']].compute()
	teams = teams[teams['LeagueId'].isin(leagues)]['TeamId']
	teams = teams.astype('string')

	return(season, teams, leagues, exportDate)

def simplifyFiles(season, teams, leagues, exportDate):
	
	#Change outfilepath to desired folder. Currently default is simplifiedCSV in root folder.
	outfilepath = 'simplifiedFiles'

	seasonvalue = season[0:4] + '-' + season[5:9]

	def sanitizeData():
	#Check conferences.csv, divisions.csv and league_data.csvs for correct number of non-null per row, check if the cleagues are in the specified leagues list, set datatypes before double merging into one dataframe, order columns and then export to league file.
	#Check player_master.csv if players have specified TeamId from specified LeagueId. Check player_ratings.csv for correct number of non-null per row. Then drop PlayerId with '-1' values as they are either duplicates or not needed. Set data type for both, merge into one dataframe, order columns and then export to players file. 
	#Check player_skater_stats_rs.csv for correct number of non-null per row, check if players have specified TeamId from specified LeagueId. Set data types and order columns before exporting to skater_stats_rs file.
	#Check player_skater_stats_po.csv for correct number of non-null per row, check if players have specified TeamId from specified LeagueId. Set data types and order columns before exporting to skater_stats_po file.
	#Check player_skater_stats_ps.csv for correct number of non-null per row, check if players have specified TeamId from specified LeagueId. Set data types and order columns before exporting to skater_stats_ps file.
	#Check player_goalie_stats_rs.csv for correct number of non-null per row, check if players have specified TeamId from specified LeagueId. Convert SV_Per 00nan value to 0. Set data types and order columns before exporting to goalie_stats_rs file.
	#Check player_goalie_stats_po.csv for correct number of non-null per row, check if players have specified TeamId from specified LeagueId. Convert SV_Per 00nan value to 0. Set data types and order columns before exporting to goalie_stats_po file.
	#Check player_goalie_stats_ps.csv for correct number of non-null per row, check if players have specified TeamId from specified LeagueId. Convert SV_Per 00nan value to 0. Set data types and order columns before exporting to goalie_stats_ps file.
	#Check player_contract_renewed.csv for correct number of non-null per row, check if players have specified TeamId from specified LeagueId. Assign Season to dataframe. Replace -1 with None and - with No. Set data types and order columns before exporting to contract_renewed file.
	#Check contract.csv for correct number of non-null per row, check if players have specified TeamId from specified LeagueId. Assign Season to dataframe. Replace -1 with None and - with No. Set data types and order columns before exporting to contract file.
	#Check team_data.csv for correct number of non-null per row, check if teams have specified TeamId from specified LeagueId. Replace -1 with None. Set data types and order columns before exporting to teams file.
	#team_lines.csv has 4 extra null values at end of each row. Check if players have specified TeamId from specified LeagueId. Set data types and order columns before exporting to team_lines file.
	#team_stats_playoffs.csv currently duplicates regular season stats. Check for correct number of non-null per row, check if players have specified TeamId from specified LeagueId. Set data types and order columns before exporting to team_stats_playoffs file.
	#Check team_stats.csv for correct number of non-null per row, check if players have specified TeamId from specified LeagueId. Check team_records.csv for correct number of non-null per row. Drop columns LeagueId, ConfId, DivId so when merging it does not cause duplicates. Set data type for both, merge into one dataframe, order columns and then export to team_stats file. 
	#Check schedules.csv for correct number of non-null per row, check if schedule is for specified LeagueId. Check boxscore_summary.csv for correct number of non-null per row, check if AwayId is from specified TeamId from specified LeagueId. Drop columns AwayId, HomeId, Date_Year, Date_Month, Date_Day, Score_Home, Score_Away, Type so when merging it does not cause duplicates. Set data type for both, merge into one dataframe, order columns and then export to games_result file. 
	#Check boxscore_period_scoring_summary.csv for correct number of non-null per row, check if TeamId have specified TeamId from specified LeagueId. Set data types and order columns before exporting to games_scores file.
	#Check boxscore_period_penalties_summary.csv for correct number of non-null per row, check if TeamId have specified TeamId from specified LeagueId. Set data types and order columns before exporting to games_penalties file.
	#Check boxscore_goalie_summary.csv for correct number of non-null per row, check if TeamId have specified TeamId from specified LeagueId. Convert SV_Per 00nan value to 0. Set data types, merge with dfSchedules, which was previously sanitized and filter out rows where Played column is not 0, order columns before exporting to goalie_stats_game file.
	#Check boxscore_skater_summary.csv for correct number of non-null per row, check if TeamId have specified TeamId from specified LeagueId. Set data types, merge with dfSchedules, which was previously sanitized and filter out rows where Played column is not 0, order columns before exporting to skater_stats_game file.
	#Check staff_master.csv for null values and fill -1 for null values in StaffId. Drop all StaffId = -1. Then drop Retired with value 1 as they are retired. staff_ratings.csv has ; at end of column headers on first line. Also use_cols to drop 'Blank' after naming third from the last column 'Blank' as there are too many columns. Check staff_ratings.csv for null values and fill StaffId with -1. Then drop StaffId with '-1' values as they are either duplicates or not needed. Drop duplicates as the files contain ~700 duplicate StaffIds at end of CSV. Set data type for both, merge into one dataframe, order columns and then export to staff file. 
	#Check draft_info.csv for correct number of non-null per row. Check draft_index.csv for correct number of non-null per row. Set data type for both, merge into one dataframe. Add +1 to dfDraft Year and turn into Season and add back to dataframe. Order columns and then export to draft file. 
	#Check player_rights.csv for correct number of non-null per row, check if TeamId have specified TeamId from specified LeagueId. Set data types and order columns before exporting to player_rights file.

		def operationsList(dfData, operation, leagues, teams):
			#Count operation checks number of non-null value and drops them if they do not equal a specified number.
			if operation['commandName'] == 'count':
				dfData = dfData[dfData.count(axis=operation['axis']) == int(operation['numCols'])]
			#isin operation checks if value is in specified column, if LeagueId checks for leagues list, if TeamId checks teams list.
			elif operation['commandName'] == 'isin':
				for isinComm in operation['commValue']:
					if isinComm['colName'] == 'LeagueId':
						dfData = dfData[dfData['LeagueId'].isin(leagues)]
					elif isinComm['colName'] == 'TeamId':
						dfData = dfData[dfData['TeamId'].isin(teams)]
					else:
						dfData = dfData[dfData[isinComm['colName']].isin([isinComm['colValue']])]
			#astype operation sets values in columns. If range == Per loops through list of columns. Else sets all columns to the same value.
			elif operation['commandName'] == 'astype':
				if operation['range'] == 'Per':
					for asTypeComm in operation['commValue']:
						if asTypeComm['colValue'] =='GR' or asTypeComm['colValue'] == 'GROff' or asTypeComm['colValue'] == 'GRDef':
							dfData = dfData.astype({asTypeComm['colName']: 'Float64'})
							dfData = dfData.astype({asTypeComm['colName']: asTypeComm['colValue']})
						else:
							dfData = dfData.astype({asTypeComm['colName']: asTypeComm['colValue']})
				else:
					dfData = dfData.astype(operation['commValue'][0]['colValue'])

			#assign operation inserts new column. If Season assigns season value line 50, if Date assigns exportDate line 57.
			elif operation['commandName'] == 'assign':
				if operation['colName'] == 'Season':
					dfData = dfData.assign(Season=season)
				elif operation['colName'] == 'Dates':
					dfData = dfData.assign(Dates=exportDate)
			#like isin operation but opposite.
			elif operation['commandName'] == 'isnotin':
				for isnotinComm in operation['commValue']:
					dfData = dfData[~dfData[isnotinComm['colName']].isin([isnotinComm['colValue']])]
			#order operation takes a list and reorders columns
			elif operation['commandName'] == 'order':
				dfData = dfData[operation['commValue']]
			#replace operation replaces value with another value. If All looks in every column else looks for specific column.
			elif operation['commandName'] == 'replace':
				for replaceComm in operation['commValue']:
					if replaceComm['colName'] == 'All':
						if replaceComm['replacewith'] == 'None':
							dfData = dfData.replace(replaceComm['replace'], None)
						else:
							dfData = dfData.replace(replaceComm['replace'], replaceComm['replacewith'])
					else:
						if replaceComm['replacewith'] == 'None':
							dfData = dfData.replace(replaceComm['replace'], None)
						else:
							dfData[replaceComm['colName']] = dfData[replaceComm['colName']].replace(replaceComm['replace'], replaceComm['replacewith'])
			#fillna operation fills in null values. If range == Per loop through list and change per column else look in every column.
			elif operation['commandName'] == 'fillna':
				if operation['range'] == 'Per':
					for fillnaComm in operation['commValue']:
						dfData = dfData.fillna(value = {fillnaComm['colName']: fillnaComm['colValue']})
				else:
					dfData = dfData.fillna(operation['commValue'][0]['colValue'])
			#drop operation takes column name and removes it from dataframe.
			elif operation['commandName'] == 'drop':
				for dropColumn in operation['commValue']:
						dfData = dfData.drop(columns = [dropColumn['colName']])
			#dropduplicates operation removes duplicates from specified column.
			elif operation['commandName'] == 'dropduplicates':
				if operation['colName'] == 'All':
					dfData = dfData.drop_duplicates()
				else:
					dfData = dfData.drop_duplicates(subset=operation['colName'])
			#draftyear operation is used to change draftyear into season. e.g 2024 -> 2024/2025 to maintain uniformity with other dataframes.
			elif operation['commandName'] == 'draftyear':
				#Change if year is past 3000 or before 1700.
				dfData['Year'] = dfData['Year'].astype(int)
				dfData = dfData[dfData['Year'] < 3000]
				dfData = dfData[dfData['Year'] > 1700]
				Year1 = dfData['Year']
				Year2 = dfData['Year'] + 1
				dfData['Season'] = Year1.astype(str) + '/' + Year2.astype(str)
			else:
				print(f'operation, {operation['commandName']}, not found.')
			return dfData

		fileCount = 0

		#import simplifying configuration file to put through the extract, sanitize, merge and export functions.
		with open('configure.json', 'r') as file:
			data = json.load(file)

		filesData = data['Files']
		dfDataFrms = [0, 1, 2, 3]

		#Loop through defined files in json configuration file. Files found in export folder of saved games.
		for fileData in filesData:
			#Check if there is an Extract structure
			if 'Extract' in fileData:
				dfData = dd.read_csv(f'csv/import_export/{fileData['Extract']['fileName']}', blocksize=fileData['Extract']['blocksize'], sep=';', on_bad_lines='skip', encoding='ISO-8859-15', header=0, names=fileData['Extract']['names'], usecols=fileData['Extract']['usecols'], dtype=fileData['Extract']['dtype'])
			#Check if there are operations to perform
			if "Operations" in fileData:
				for operation in fileData['Operations']:
					dfData = operationsList(dfData, operation, leagues, teams)

			#Store manipulated dataframes in specified Index for later output or retrieval.
			dfDataFrms[fileData['PreStoreIndex']] = dfData
			#Check if there are merge operations to perform
			if 'Merges' in fileData:
				dfDataFrms[fileData['Merges']['PostStoreIndex']] = dd.merge(dfDataFrms[fileData['Merges']['mergeIndexes'][0]], dfDataFrms[fileData['Merges']['mergeIndexes'][1]], on = fileData['Merges']['on'], how=fileData['Merges']['how'])
				#"Pop" Index but actually just insert number x to remove data from that index but keep index defined.
				for x in fileData['Merges']['popIndex']:
					dfDataFrms[x] = x
			#Check if there are operations to do post merge. Same kind of operations that can be performed before a merge.
			if 'PostMergesOperations' in fileData:
				for PostMergesOperations in fileData['PostMergesOperations']:
					dfDataFrms[0] = operationsList(dfDataFrms[0], PostMergesOperations, leagues, teams)
			#Check if dataframe will be outputed to CSV. Not every file will be exported may loop through and merge with another file.
			if "Output" in fileData:
				dfDataFrms[0].to_csv(f'{outfilepath}/csv/{fileData['Output']}.csv', index=False, single_file=True)
				dfDataFrms[0].to_parquet(f'{outfilepath}/parquet/{fileData['Output']}.parquet', name_function=lambda x: f'{seasonvalue}_{fileData['Output']}{x}.parquet', write_index=False)	
				#overwrite first element which should always be final store location before exporting to csv or parquet.
				dfDataFrms[0] = 0
				fileCount += 1
				printProgressBar(fileCount, 21, prefix = fileData['Output'])

	sanitizeData()

def main():
	files = importFiles()
	season, teams, leagues, exportDate = getLeagues(files[0], files[1])
	simplifyFiles(season, teams, leagues, exportDate)

if __name__ == '__main__':
	main()