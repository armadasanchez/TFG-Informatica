import pandas as pd
import numpy as np
from datetime import datetime
import json


typeMapping = ['Sandbox~SAND', '1. One Box~Tutorial', '2. Separated Boxes~Tutorial', '3. Rotate a Pyramid~Tutorial', '4. Match Silhouettes~Tutorial', '5. Removing Objects~Tutorial', '6. Stretch a Ramp~Tutorial', '7. Max 2 Boxes~Tutorial', '8. Combine 2 Ramps~Tutorial', '9. Scaling Round Objects~Tutorial',
               'Square Cross-Sections~Easy Puzzles', 'Bird Fez~Easy Puzzles', 'Pi Henge~Easy Puzzles', '45-Degree Rotations~Easy Puzzles',  'Pyramids are Strange~Easy Puzzles', 'Boxes Obscure Spheres~Easy Puzzles', 'Object Limits~Easy Puzzles', 'Not Bird~Easy Puzzles', 'Angled Silhouette~Easy Puzzles',
               'Warm Up~Hard Puzzles','Tetromino~Hard Puzzles', 'Stranger Shapes~Hard Puzzles', 'Sugar Cones~Hard Puzzles', 'Tall and Small~Hard Puzzles', 'Ramp Up and Can It~Hard Puzzles', 'More Than Meets Your Eye~Hard Puzzles', 'Unnecessary~Hard Puzzles', 'Zzz~Hard Puzzles', 'Bull Market~Hard Puzzles', 'Few Clues~Hard Puzzles', 'Orange Dance~Hard Puzzles', 'Bear Market~Hard Puzzles']

tutorialPuzzles = []

for puzzle in typeMapping:
    desc = puzzle.split("~")
    if(desc[1] == 'Tutorial'):
        tutorialPuzzles.append(desc[0])

advancedPuzzles = []

for puzzle in typeMapping:
    desc = puzzle.split("~")
    if(desc[1] == 'Hard Puzzles'):
        advancedPuzzles.append(desc[0])


intermediatePuzzles = []

for puzzle in typeMapping:
    desc = puzzle.split("~")
    if(desc[1] == 'Easy Puzzles'):
        intermediatePuzzles.append(desc[0])

allPuzzles = []
for puzzle in typeMapping:
    desc = puzzle.split("~")
    allPuzzles.append(desc[0])



def computeUserHistorial(dataEvents, group, user,timeLimit = pd.to_datetime('2022-09-10 13:40:17.975299-04:00',utc=True) ):



    dataEvents['time'] = pd.to_datetime(dataEvents['time'],utc=True)
    dataEvents = dataEvents.sort_values('time')
    dataEvents['group'] = [json.loads(x)['group'] if 'group' in json.loads(x).keys() else '' for x in dataEvents['data']]
    dataEvents['user'] = [json.loads(x)['user'] if 'user' in json.loads(x).keys() else '' for x in dataEvents['data']]

    # removing those rows where we dont have a group and a user that is not guest
    dataEvents = dataEvents[((dataEvents['group'] != '') & (dataEvents['user'] != '') & (dataEvents['user'] != 'guest'))]
    dataEvents['group_user_id'] = dataEvents['group'] + '~' + dataEvents['user']

    # filtering to only take the group passed as argument
    if(group != 'all'):
        dataEvents = dataEvents[dataEvents['group'].isin(group)]
    # filtering to only take the user passed as argument
    if(user != 'all'):
        dataEvents = dataEvents[dataEvents['user'].isin(user)]
    # filtering to only take the events before the timeLimit
    dataEvents = dataEvents.loc[dataEvents['time'] < timeLimit]


    completed = dict()
    tutorialCompleted = dict()
    intermediateCompleted = dict()
    advancedCompleted = dict()
    users = []
    userAttempts = dict()
    userWins = dict()

    key = user[0]
    print(user[0])

    tutorialCompleted[key] = 0
    intermediateCompleted[key] = 0
    advancedCompleted[key] = 0
    userAttempts[key] = 0
    userWins[key] = 0

    user_events = dataEvents[dataEvents['group_user_id'] == key]
    user_puzzle_key = None

    for enum, event in user_events.iterrows():

        if(event['type'] in ['ws-start_level', 'ws-puzzle_started']):
            user_puzzle_key = event['group'] + '~' + event['user'] + '~' + json.loads(event['data'])['task_id']
            if(user_puzzle_key not in completed.keys()): completed[user_puzzle_key] = 0

        if(event['type'] == 'ws-puzzle_started'):
            userAttempts[key] +=1
        elif(event['type'] == 'ws-puzzle_complete'):
            userWins[key] += 1
            if completed[user_puzzle_key] == 0 :
                llave = user_puzzle_key.split("~")
                puzzle = llave[2]
                if (puzzle in tutorialPuzzles):
                    tutorialCompleted[key] += 1
                elif (puzzle in intermediatePuzzles):
                    intermediateCompleted[key] += 1
                elif (puzzle in advancedPuzzles):
                    advancedCompleted[key] += 1
            completed[user_puzzle_key] = 1

    resultado = dict()

    if userAttempts[key] == 0 or userWins[key] == 0:
        attempts_per_puzzle = 0
    else: attempts_per_puzzle = round((userAttempts[key]/userWins[key]),2)

    resultado['percentage_tutorial'] = round((tutorialCompleted[key]/9)*100,2)
    resultado['percentage_intermediate'] = round((intermediateCompleted[key]/9)*100,2)
    resultado['percentage_advanced'] = round((advancedCompleted[key]/13)*100,2)
    resultado['attempts_per_puzzle'] = attempts_per_puzzle

    return resultado
