import pandas as pd
import json

def computeTimeLimitEvents(dataEvents,timeLimit = pd.to_datetime('2022-09-10 13:40:17.975299-04:00',utc=True) ):

    dataEvents['time'] = pd.to_datetime(dataEvents['time'],utc=True)
    dataEvents = dataEvents.sort_values('time')

    # filtering to only take the events before the timeLimit
    dataEvents = dataEvents.loc[dataEvents['time'] < timeLimit]

    return dataEvents