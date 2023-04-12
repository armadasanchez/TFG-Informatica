import pandas as pd
import json

def computeTimeLimitEvents(dataEvents, user_objective = 'all', timeLimit = pd.to_datetime('2022-09-10 13:40:17.975299-04:00',utc=True) ):

    dataEvents['time'] = pd.to_datetime(dataEvents['time'],utc=True)
    dataEvents = dataEvents.sort_values('time')

    # filtering to only take the events before the timeLimit
    dataEvents = dataEvents.loc[dataEvents['time'] <= timeLimit]
    if user_objective != 'all':
        dataEvents['user'] = [json.loads(x)['user'] if 'user' in json.loads(x).keys() else '' for x in dataEvents['data']]
        dataEvents = dataEvents[dataEvents['user'].isin(user_objective)]
        dataEvents = dataEvents.drop(columns=['user'])

    return dataEvents