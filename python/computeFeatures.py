import pandas as pd
from collections import OrderedDict
import numpy as np
import json
import time
from sklearn.preprocessing import OneHotEncoder

import sys
sys.path.append('../python')
import userHistorial as uh
import ELO as elo
import timeLimitEvents as timeLimits

# Dictionary with the average complete time of every puzzle
tiemposPuzzles = pd.read_csv('E:/Documentos/PCEO/5/Informatica/TFG/scripts/TFG-Informatica/Outputs/avgTimeByPuzzleOutput.csv', sep=";")

# Dictionary with the ELO difficulty of every puzzle
dict_elo_puzzles = {
    'Sandbox': 0.0,
    '1. One Box': 0.0,
    '2. Separated Boxes': 0.12,
    '3. Rotate a Pyramid': 0.24,
    '4. Match Silhouettes': 0.42,
    'Sugar Cones': 0.9,
    '8. Combine 2 Ramps': 0.57,
    '9. Scaling Round Objects': 0.57,
    'Square Cross-Sections': 0.99,
    'Bird Fez': 1.77,
    'Pi Henge': 1.26,
    '45-Degree Rotations': 0.78,
    'Pyramids are Strange': 1.29,
    'Boxes Obscure Spheres': 1.91,
    'Object Limits': 1.62,
    'Tetromino': 1.74,
    'Angled Silhouette': 1.35,
    'Stranger Shapes': 1.7,
    'Tall and Small': 1.73,
    '5. Removing Objects': 0.48,
    '6. Stretch a Ramp': 0.42,
    '7. Max 2 Boxes': 0.6,
    'Ramp Up and Can It': 1.73,
    'More Than Meets Your Eye': 1.2,
    'Bear Market': 3.0,
    'Not Bird': 2.13,
    'Warm Up': 0.6,
    'Unnecessary': 1.74,
    'Zzz': 1.81,
    'Bull Market': 2.98,
    'Few Clues': 1.93,
    'Orange Dance': 2.37
}

# Every puzzle difficulty
difficultyMapping = ['Sandbox~0.000001','1. One Box~0.000002', '2. Separated Boxes~0.111127', '3. Rotate a Pyramid~0.083447', '4. Match Silhouettes~0.061887', '5. Removing Objects~0.106021', '6. Stretch a Ramp~0.107035', '7. Max 2 Boxes~0.078039', '8. Combine 2 Ramps~0.068608', '9. Scaling Round Objects~0.128647',
                     'Square Cross-Sections~0.199714', 'Bird Fez~0.156674', 'Pi Henge~0.067346', '45-Degree Rotations~0.096715',  'Pyramids are Strange~0.179600', 'Boxes Obscure Spheres~0.266198', 'Object Limits~0.257177', 'Not Bird~0.260197', 'Angled Silhouette~0.147673',
                     'Warm Up~0.183971','Tetromino~0.226869', 'Stranger Shapes~0.283971', 'Sugar Cones~0.085909', 'Tall and Small~0.266869', 'Ramp Up and Can It~0.206271', 'More Than Meets Your Eye~0.192319', 'Unnecessary~0.76', 'Zzz~0.234035', 'Bull Market~0.358579', 'Few Clues~0.324041', 'Orange Dance~0.647731', 'Bear Market~1.000000']

# Dictionary with the difficulty of every puzzle
difficultyPuzzles = dict()
for puzzle in difficultyMapping:
    desc = puzzle.split("~")
    difficultyPuzzles[desc[0]] = float(desc[1])


def computeFeatures(dataEvents,percentil=1, user_objective='all', group_objective = 'all'):

    # Guardar el instante inicial
    inicio = time.time()

    #copia
    copia = dataEvents.copy()

    dataEvents['time'] = pd.to_datetime(dataEvents['time'])
    dataEvents = dataEvents.sort_values('time')

    #iterates in the groups and users of the data
    dataEvents['group'] = [json.loads(x)['group'] if 'group' in json.loads(x).keys() else '' for x in
                           dataEvents['data']]
    dataEvents['user'] = [json.loads(x)['user'] if 'user' in json.loads(x).keys() else '' for x in dataEvents['data']]
    dataEvents['task_id'] = [json.loads(x)['task_id'] if 'task_id' in json.loads(x).keys() else '' for x in
                             dataEvents['data']]

    # removing those rows where we dont have a group and a user that is not guest
    dataEvents = dataEvents[
        ((dataEvents['group'] != '') & (dataEvents['user'] != '') & (dataEvents['user'] != 'guest'))]
    dataEvents['group_user_id'] = dataEvents['group'] + '~' + dataEvents['user']
    dataEvents['group_user_task_id'] = dataEvents['group'] + '~' + dataEvents['user'] + '~' + dataEvents['task_id']

    # filtering to only take the user passed as argument
    #if(user != 'all'):
    #    dataEvents = dataEvents[dataEvents['user'].isin(user)]
    if(group_objective != 'all'):
        dataEvents = dataEvents[dataEvents['group'].isin(group_objective)]

    # the data is grouped by the necessary variables
    activity_by_user = dataEvents.groupby(['group_user_id']).agg({'id': 'count',
                                                                  'type': 'nunique'}).reset_index().rename(columns={'id': 'events',
                                                                                                                    'type': 'different_events'})



    # Data Cleaning
    #dataEvents['time'] = pd.to_datetime(dataEvents['time'])
    dataEvents = dataEvents.sort_values('time')

    typeEvents = ['ws-snapshot', 'ws-paint', 'ws-rotate_view', 'ws-move_shape', 'ws-rotate_shape', 'ws-scale_shape',
                  'ws-create_shape', 'ws-delete_shape', 'ws-undo_action', 'ws-redo_action', 'ws-check_solution']
    manipulationTypeEvents = ['ws-move_shape', 'ws-rotate_shape', 'ws-scale_shape', 'ws-create_shape',
                              'ws-delete_shape']

    #initialize the metrics
    activity_by_user['completed'] = np.nan
    activity_by_user['active_time'] = np.nan
    activity_by_user['n_events'] = np.nan
    activity_by_user['timestamp'] = np.nan

    for event in typeEvents:
        activity_by_user[event] = 0
    #----------------------------------------------------------------------------------------------------------------------------------
    #initialize the data structures

    puzzleEvents = dict() #Diccionario que guarda para cada intento (alumno,puzzle,nintento) el número de eventos

    timePuzzle = dict() #Diccionario que guarda para cada intento el tiempo activo

    globalTypesEvents = dict() #Diccionario que guarda para cada intento el número de eventos de cada tipo

    n_attempts = dict() #Diccionario que guarda para cada el número de intentos

    completados = dict() #Diccionario que guarda para cada intento si se completó

    timestamp = dict() #Diccionario que guarda para cada intento el timestamp inicial

    percentilAtt = dict()
    percentilTime = dict() #Diccionarios que guardan un 90 para cada intento (creo)
    percentilAttValue = 90
    percentilTimeValue = 90

    breaksPuzzle = dict() #Diccionario que guarda para cada intento el número de breaks ( parones de 15 segundos )

    cumAttempts = OrderedDict() #Diccionario que guarda para cada intento, que número de intento gloabal es


    userCumAttempts = OrderedDict() #Diccionario que guarda para cada alumno cuantos intentos en total ha hecho


    prevReg = dict() #Diccionario que almacena para cada dupla alumno-puzzle, si se ha intentado anteriormente

    actualAtt = 0 #Variable global que lleva el contador de intentos global

    idComplete = dict() #Diccionario que guarda para cada intento si se finalizó

    attemptsAux = dict() #Doble diccionario que guarda para cada usuario y para cada puzzle el numero de intentos

    contCheckSol = dict() #Diccionario que guarda para cada intento el número de submits

    bestSubmit = dict() #Diccionario que guarda el mejor submit de cada intento

    manipulationEvents = dict() #Diccionario que guarda para cada intento el número de eventos de manipulacion

    userManipulationEvents = dict() #Creo que actualmente no se utiliza

    contManipulation = 0

    timeFirstCheck = dict() #Diccionario que guarda para cada intento el timestamp del primer submit

    timeSubExit = dict() #Diccionario que guarda para cada intento, el tiempo que ha pasado entre el primer submit y el exit

    timeCheckActual = dict() #Diccionario que guarda para cada intento el timestamp del último check

    timeBetweenSub = dict() #Diccionario que guarda para cada intento la media de tiempo entre cada submit
    #---------------------------------------------------------------------------------------------------------------------------------
    for user in dataEvents['group_user_id'].unique():

        # Computing active time
        previousEvent = None
        theresHoldActivity = 60
        tiempoPercentil = 0

        user_events = dataEvents[dataEvents['group_user_id'] == user] #eventos del usuario
        user_puzzle_key = None
        userParc = None
        task_id = None
        initialTime = None
        prev_id = 1

        for enum, event in user_events.iterrows():

            # If it is the first event
            if (previousEvent is None):
                previousEvent = event
                continue

            if (event['type'] in ['ws-start_level']):

                #create id: group+user+task_id
                task_id = json.loads(event['data'])['task_id']

                if (user_puzzle_key not in timeSubExit.keys()):
                    timeSubExit[user_puzzle_key] = str(0)
                    timeBetweenSub[user_puzzle_key] = str(0)

                if (event['user'] not in userCumAttempts.keys()):
                    userCumAttempts[event['user']] = 0
                    actualAtt = 0
                    attemptsAux[event['user']] = dict()
                    timeCheckActual[event['user']] = 0

                if (event['user'] not in userManipulationEvents.keys()):
                    userManipulationEvents[event['user']] = 0


                if (task_id not in attemptsAux[event['user']].keys()): attemptsAux[event['user']][task_id] = 0

                user_puzzle_key = event['group'] + '~' + event[
                    'user'] + '~' + task_id  # + '~' + str(n_attempts[prev_id])
                if (user_puzzle_key not in prevReg.keys()):

                    prevReg[user_puzzle_key] = 1
                    user_puzzle_key = event['group'] + '~' + event['user'] + '~' + task_id + '~' + '1'
                    n_attempts[user_puzzle_key] = 1
                    attemptsAux[event['user']][task_id] = n_attempts[user_puzzle_key]

                else:

                    user_puzzle_key = event['group'] + '~' + event['user'] + '~' + task_id + '~' + str(
                        attemptsAux[event['user']][task_id])
                    n_attempts[user_puzzle_key] = attemptsAux[event['user']][task_id]

                key_split = user_puzzle_key.split('~')
                puzzleParc = key_split[2]
                userParc = key_split[1]

                tiemposPuzzle = tiemposPuzzles[tiemposPuzzles['puzzle']==puzzleParc].reset_index()
                if not tiemposPuzzle.empty:
                    tiempoPercentil =  round(tiemposPuzzle.loc[0,'avg_complete_time']*0.25*percentil,2)

                if (user_puzzle_key not in idComplete.keys()): idComplete[user_puzzle_key] = 0

                if (task_id not in attemptsAux[userParc].keys()): attemptsAux[userParc][task_id] = 0
                if (user_puzzle_key not in cumAttempts.keys()): cumAttempts[user_puzzle_key] = 1

                # initialize if the id is new
                if (user_puzzle_key not in puzzleEvents.keys()):

                    breaksPuzzle[user_puzzle_key] = 0
                    timestamp[user_puzzle_key] = 0
                    percentilAtt[user_puzzle_key] = percentilAttValue
                    percentilTime[user_puzzle_key] = percentilTimeValue
                    completados[user_puzzle_key] = 0
                    puzzleEvents[user_puzzle_key] = 1
                    timePuzzle[user_puzzle_key] = 0
                    contCheckSol[user_puzzle_key] = 0
                    bestSubmit[user_puzzle_key] = 0
                    manipulationEvents[user_puzzle_key] = 0
                    timeFirstCheck[user_puzzle_key] = 0

                    globalTypesEvents[user_puzzle_key] = dict()
                    for ev in typeEvents:
                        globalTypesEvents[user_puzzle_key][ev] = 0

                #timestamp
                if (event['type'] in 'ws-start_level'):
                    timestamp[user_puzzle_key] = event['time']

            # the event is not final event
            if (event['type'] not in ['ws-exit_to_menu', 'ws-disconnect', 'ws-create_user', 'ws-login_user']):

                #calculate the duration of the event
                delta_seconds = (event['time'] - previousEvent['time']).total_seconds()

                if ((delta_seconds < theresHoldActivity)):
                    timePuzzle[user_puzzle_key] += delta_seconds



                if (event['type'] in ['ws-puzzle_complete']): completados[user_puzzle_key] = 1



                if round((timePuzzle[user_puzzle_key]/60),2) < tiempoPercentil:

                    puzzleEvents[user_puzzle_key] += 1

                    #breaks
                    if ((delta_seconds > 15)):
                        breaksPuzzle[user_puzzle_key] += 1

                    #update event counters by type
                    if (event['type'] in typeEvents):
                        globalTypesEvents[user_puzzle_key][event['type']] += 1

                    if (globalTypesEvents[user_puzzle_key]['ws-check_solution'] == 1): timeFirstCheck[user_puzzle_key] = \
                        event['time']

                    if (event['type'] in manipulationTypeEvents):
                        manipulationEvents[user_puzzle_key] += 1

                    if (event['type'] == 'ws-check_solution'):
                        timeCheckActual[event['user']] = event['time']
                        contCheckSol[user_puzzle_key] += 1
                        dict_views = json.loads(event['data'])['correct']
                        corr = 0
                        for key in dict_views:
                            if key == True:
                                corr += 1
                        percentage_aux = (corr / len(dict_views))*100
                        if percentage_aux > bestSubmit[user_puzzle_key]:
                            bestSubmit[user_puzzle_key] = round(percentage_aux,2)






                previousEvent = event



            # the puzzle ends
            if (event['type'] in ['ws-exit_to_menu', 'ws-disconnect']):

                idComplete[user_puzzle_key] = 1

                #calculate the duration of the event
                delta_seconds = (event['time'] - previousEvent['time']).total_seconds()
                if ((delta_seconds < theresHoldActivity)):
                    timePuzzle[user_puzzle_key] += delta_seconds



                if round((timePuzzle[user_puzzle_key]/60),2) < tiempoPercentil:

                    puzzleEvents[user_puzzle_key] += 1

                    #breaks
                    if ((delta_seconds > 15)):
                        breaksPuzzle[user_puzzle_key] += 1


                if (completados[user_puzzle_key] == 0 and globalTypesEvents[user_puzzle_key]['ws-check_solution'] > 0):
                    timeSubExit[user_puzzle_key] = str(
                        round((event['time'] - timeFirstCheck[user_puzzle_key]).total_seconds(), 2))
                else:
                    timeSubExit[user_puzzle_key] = 'NA'

                if (globalTypesEvents[user_puzzle_key]['ws-check_solution'] == 0):
                    timeBetweenSub[user_puzzle_key] = 'NA'
                else:
                    timeBetweenSub[user_puzzle_key] = str(round(((timeCheckActual[event['user']] - timestamp[
                        user_puzzle_key]) / globalTypesEvents[user_puzzle_key]['ws-check_solution']).total_seconds(),
                                                                2))

                previousEvent = event

                userCumAttempts[userParc] += 1
                n_attempts[user_puzzle_key] += 1
                actualAtt += 1
                cumAttempts[user_puzzle_key] = actualAtt
                attemptsAux[userParc][task_id] = n_attempts[user_puzzle_key]


    # Guardar el instante final
    fin = time.time()

    # Calcular la diferencia en segundos
    diferencia = fin - inicio

    print("El proceso 1 tardó", diferencia, "segundos.")

    count = 0

    for i in puzzleEvents.keys():

        if(idComplete[i]==0):
            continue

        key_split = i.split('~')

        if(user_objective != 'all'):
            if not (key_split[1] in user_objective):
                continue
        if(group_objective != 'all'):
            if not (key_split[0] in group_objective):
                continue


        # recortamos los eventos hasta el timestamp inicial del intento
        data_events_aux = timeLimits.computeTimeLimitEvents(copia,user_objective=[key_split[1]], timeLimit= timestamp[i])

        # recuperamos el historial del usuario
        user_historial = uh.computeUserHistorial(data_events_aux,group = [key_split[0]], user = [key_split[1]])


        # recuperamos elo del jugador y dificultad ELO del puzzle

        totalData = elo.adaptedData(data_events_aux)
        if "user" in totalData.columns:
            user_elo = elo.run(1.8, 0.05, totalData, user_objective =key_split[1], puzzle_objective=key_split[2], group_objective = key_split[0])
        else:
            user_elo = 0
        puzzle_elo = dict_elo_puzzles[key_split[2]]
        print('timestamp:' + str(timestamp[i]))
        print('elo: ' + str(user_elo) + ' puzzle: ' + str(puzzle_elo))


        if(key_split[2] != '' and key_split[1] != '' and i != ''):

            # User, group and puzzle
            activity_by_user.at[i, 'group'] = key_split[0] # Grupo
            activity_by_user.at[i, 'user'] = key_split[1] # Usuario
            activity_by_user.at[i, 'task_id'] = key_split[2] # Puzzle
            activity_by_user.at[i, 'cum_this_puzzle_attempt'] = key_split[3] # Numero de intento de este usuario en este puzzle
            activity_by_user.at[i, 'timestamp'] = timestamp[i] # Timestamp inicial del intento

            # User historial. Percentage of tutorial,intermediate and advanced per puzzle. Attempts per puzzle
            activity_by_user.at[i, 'percentage_tutorial'] = user_historial['percentage_tutorial']
            activity_by_user.at[i, 'percentage_intermediate'] = user_historial['percentage_intermediate']
            activity_by_user.at[i, 'percentage_advanced'] = user_historial['percentage_advanced']
            activity_by_user.at[i, 'attempts_per_puzzle'] = user_historial['attempts_per_puzzle']

            # User and puzzle ELO
            activity_by_user.at[i, 'user_elo'] = user_elo
            activity_by_user.at[i, 'puzzle_elo'] = puzzle_elo

            # Puzzle difficulty
            activity_by_user.at[i, 'puzzle_difficulty'] = round(difficultyPuzzles[key_split[2]],2)

            # features of the attempt
            activity_by_user.at[i, 'n_events'] = puzzleEvents[i] #numero de eventos
            activity_by_user.at[i, 'n_check_solution'] = globalTypesEvents[i]['ws-check_solution'] #numero de submits
            activity_by_user.at[i, 'bestSubmit'] = bestSubmit[i] #numero de submits
            activity_by_user.at[i, 'n_breaks'] = breaksPuzzle[i] #numero de breaks en el intento
            activity_by_user.at[i, 'n_manipulation_events'] = manipulationEvents[i] #numero de intentos de manipulacion
            activity_by_user.at[i, 'n_snapshot'] = globalTypesEvents[i]['ws-snapshot']  #numero de snaphshots
            activity_by_user.at[i, 'n_rotate_view'] = globalTypesEvents[i]['ws-rotate_view'] #numero de rotaciones
            activity_by_user.at[i, 'time_failed_submission_exit'] = timeSubExit[i] #tiempo entre submit y exit
            activity_by_user.at[i, 'avg_time_between_submissions'] = timeBetweenSub[i] #tiempo entre submits

            # label of the attempt
            activity_by_user.at[i, 'completed'] = completados[i] #Ha completado el puzzle o no

        count += 1
        print('Intentos procesados: ', count)

    #delete row with NaN
    activity_by_user.dropna(subset = ['user'], inplace=True)

    #data output preparation
    activity_by_user = pd.DataFrame(activity_by_user, columns=['group', 'user','task_id','cum_this_puzzle_attempt','timestamp','percentage_tutorial','percentage_intermediate','percentage_advanced','attempts_per_puzzle','user_elo','puzzle_elo','puzzle_difficulty','n_events','n_check_solution','bestSubmit','n_breaks','n_manipulation_events','n_snapshot','n_rotate_view','completed'])

    # Crear una instancia del codificador
    encoder = OneHotEncoder()

    # Ajustar el codificador a los datos
    encoder.fit(activity_by_user[['task_id']])

    # Transformar los datos
    encoded_data = encoder.transform(activity_by_user[['task_id']]).toarray()

    # Crear un nuevo DataFrame con los datos codificados
    encoded_df = pd.DataFrame(encoded_data, columns=encoder.categories_[0])

    # Agregar las nuevas columnas al DataFrame original
    activity_by_user = activity_by_user.reset_index(drop=True)
    encoded_df = encoded_df.reset_index(drop=True)
    activity_by_user_encoded = pd.concat([activity_by_user, encoded_df], axis=1)

    return activity_by_user_encoded