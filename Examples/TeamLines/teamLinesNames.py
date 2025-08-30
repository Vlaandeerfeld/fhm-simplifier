import pandas as pd

calgaryLines = pd.read_csv('Calgary_Lines.csv')
TeamId = calgaryLines['TeamId']
Dates = calgaryLines['Dates']

def fun(comp1):
    calgaryPlayers = pd.read_csv('Calgary_Players.csv')
    simpl = calgaryPlayers[calgaryPlayers['PlayerId'].isin(comp1)].reset_index()
    if simpl is None:
        return 'None'
    return simpl['FirstName'] + ' ' + simpl['LastName']

fileOut = calgaryLines.apply(fun)
fileOut['TeamId'] = TeamId
fileOut['Dates'] = Dates

fileOut.to_csv('Calgary_Lines_Print.csv', index=False)