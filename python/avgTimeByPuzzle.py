import pandas as pd
import numpy as np
import json

pd.options.mode.chained_assignment = None  # default='warn'

orderMapping = {'1. One Box': 1, '2. Separated Boxes': 2, '3. Rotate a Pyramid': 3, '4. Match Silhouettes': 4, '5. Removing Objects': 5, '6. Stretch a Ramp': 6, '7. Max 2 Boxes': 7, '8. Combine 2 Ramps': 8, '9. Scaling Round Objects': 9,
                'Square Cross-Sections': 10, 'Bird Fez': 11, 'Pi Henge': 12, '45-Degree Rotations': 13,  'Pyramids are Strange': 14, 'Boxes Obscure Spheres': 15, 'Object Limits': 16, 'Warm Up': 17, 'Angled Silhouette': 18,
                'Sugar Cones': 19,'Stranger Shapes': 20, 'Tall and Small': 21, 'Ramp Up and Can It': 22, 'More Than Meets Your Eye': 23, 'Not Bird': 24, 'Unnecesary': 25, 'Zzz': 26, 'Bull Market': 27, 'Few Clues': 28, 'Orange Dance': 29, 'Bear Market': 30}


def computeAvgTimes(dataEvents):

    dataEvents['group'] = [json.loads(x)['group'] if 'group' in json.loads(x).keys() else '' for x in dataEvents['data']]
    dataEvents['user'] = [json.loads(x)['user'] if 'user' in json.loads(x).keys() else '' for x in dataEvents['data']]

    # removing those rows where we dont have a group and a user that is not guest
    dataEvents = dataEvents[((dataEvents['group'] != '') & (dataEvents['user'] != '') & (dataEvents['user'] != 'guest'))]
    dataEvents['group_user_id'] = dataEvents['group'] + '~' + dataEvents['user']


    # Data Cleaning
    dataEvents['time'] = pd.to_datetime(dataEvents['time'])
    dataEvents = dataEvents.sort_values('time')

    userPuzzleDict = {}
    theresHoldActivity = 60

    for user in dataEvents['group_user_id'].unique():

        #Select rows
        user_events = dataEvents[dataEvents['group_user_id'] == user]
        userPuzzleDict[user] = {}

        # Analyze when a puzzle has been started
        activePuzzle = None
        previousEvent = None
        activeTime = []

        for enum, event in user_events.iterrows():

            if(event['type'] == 'ws-start_level'):

                initialTime = event['time']
                activePuzzle = json.loads(event['data'])['task_id']
                if(activePuzzle not in userPuzzleDict[user].keys()):
                    userPuzzleDict[user][activePuzzle] = {'completed':0,'avg_complete_time':0,'TotalTime':0}

            # If event is puzzle complete we always add it
            if(event['type'] == 'ws-puzzle_complete'):
                puzzleName = json.loads(event['data'])['task_id']
                if(puzzleName in userPuzzleDict[user].keys()):
                    if(userPuzzleDict[user][puzzleName]['completed']==0):
                        userPuzzleDict[user][activePuzzle]['avg_complete_time'] += round(np.sum(activeTime)/60,2)
                        userPuzzleDict[user][activePuzzle]['TotalTime'] += round(((event['time'] - initialTime).total_seconds())/60,2)
                        userPuzzleDict[user][puzzleName]['completed'] = 1

            # If they are not playing a puzzle we do not do anything and continue
            if(activePuzzle is None):
                continue

            # If it is the first event we store the current event and continue
            if(previousEvent is None):
                previousEvent = event
                continue

            #Add new active time
            delta_seconds = (event['time'] - previousEvent['time']).total_seconds()
            if((delta_seconds < theresHoldActivity)):
                activeTime.append(delta_seconds)

            # Analyze when puzzle is finished or user left
            # Measure time, attempts, completion and actions
            if(event['type'] in ['ws-puzzle_complete', 'ws-exit_to_menu', 'ws-disconnect']):
                # reset counters
                activeTime = []
                activePuzzle = None

            previousEvent = event


    stats_by_level_player = []
    for user in userPuzzleDict.keys():
        userDf = pd.DataFrame.from_dict(userPuzzleDict[user], orient = 'index')
        userDf['group_user_id'] = user
        key_split = user.split('~')
        userDf['group'] = key_split[0]
        if (userDf.shape != 0):
            stats_by_level_player.append(userDf)
        else:
            continue

    try:
        stats_by_level_player = pd.concat(stats_by_level_player, sort=True)
        stats_by_level_player['puzzle'] = stats_by_level_player.index
        stats_by_level_player['order'] = stats_by_level_player['puzzle'].map(orderMapping)

        avgTimes = round(stats_by_level_player.groupby(['puzzle','order']).agg({
            'avg_complete_time': lambda x: np.mean(x[x!=0]),
            'TotalTime': lambda x: np.mean(x[x!=0]) })
                            .reset_index(),2).sort_values('order')


        return avgTimes

    except ValueError:
        return -1