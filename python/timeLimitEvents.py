import pandas as pd

def computeTimeLimitEvents(dataEvents, user_objective = 'all', timeLimit = pd.to_datetime('2022-09-10 13:40:17.975299-04:00',utc=True) ):
    # filtering to only take the events before the timeLimit
    dataEvents = dataEvents.loc[dataEvents['time'] <= timeLimit]
    if user_objective != 'all':
        dataEvents = dataEvents[dataEvents['user'].isin(user_objective)]
    return dataEvents