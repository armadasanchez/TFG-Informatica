from datetime import datetime
import json
import numpy as np
import pandas as pd
from sklearn import metrics
import math
import statistics
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler

student_id = 'user'
timestamp = 'initial timestamp'
student_column_number = 1
group_column_number = 0
completed = 'n_completed'
puzzle_name = 'task_id'
puzzle_column_number = 2
kc_column = 'kc'
kc_column_number = 4

kcs = ['GMD.4', 'CO.5', 'CO.6','MG.1']
mg1Puzzles = ['Bird Fez', 'Pi Henge', 'Bull Market']
gmd4Puzzles = ['Angled Silhouettes', 'Not Bird', 'Stranger Shapes', 'Ramp Up and Can It', 'Few Clues']
co5Puzzles = ['45-Degree Rotations', 'Boxes Obscure Spheres', 'More Than Meets the Eye']
co6Puzzles = ['Tall and Small', 'Not Bird', 'Ramp Up and Can It', 'Stretch a Ramp', 'Max 2 Boxes']

typeMappingDifficulty = ['Sandbox~SAND', '1. One Box~Tutorial', '2. Separated Boxes~Tutorial', '3. Rotate a Pyramid~Tutorial', '4. Match Silhouettes~Tutorial', '5. Removing Objects~Tutorial', '6. Stretch a Ramp~Tutorial', '7. Max 2 Boxes~Tutorial', '8. Combine 2 Ramps~Tutorial', '9. Scaling Round Objects~Tutorial',
               'Square Cross-Sections~Easy Puzzles', 'Bird Fez~Easy Puzzles', 'Pi Henge~Easy Puzzles', '45-Degree Rotations~Easy Puzzles',  'Pyramids are Strange~Easy Puzzles', 'Boxes Obscure Spheres~Easy Puzzles', 'Object Limits~Easy Puzzles', 'Not Bird~Easy Puzzles', 'Angled Silhouette~Easy Puzzles',
               'Warm Up~Hard Puzzles','Tetromino~Hard Puzzles', 'Stranger Shapes~Hard Puzzles', 'Sugar Cones~Hard Puzzles', 'Tall and Small~Hard Puzzles', 'Ramp Up and Can It~Hard Puzzles', 'More Than Meets Your Eye~Hard Puzzles', 'Unnecessary~Hard Puzzles', 'Zzz~Hard Puzzles', 'Bull Market~Hard Puzzles', 'Few Clues~Hard Puzzles', 'Orange Dance~Hard Puzzles', 'Bear Market~Hard Puzzles']

tutorialPuzzles = []

for puzzle in typeMappingDifficulty:
    desc = puzzle.split("~")
    if(desc[1] == 'Tutorial'):
        tutorialPuzzles.append(desc[0])
        
advancedPuzzles = []

for puzzle in typeMappingDifficulty:
    desc = puzzle.split("~")
    if(desc[1] == 'Hard Puzzles'):
        advancedPuzzles.append(desc[0])
        
        
intermediatePuzzles = []

for puzzle in typeMappingDifficulty:
    desc = puzzle.split("~")
    if(desc[1] == 'Easy Puzzles'):
        intermediatePuzzles.append(desc[0])

# mapping to positions

typeMappingKC = {'Sandbox': 'GMD.4~CO.5~CO.6', '1. One Box': 'GMD.4~CO.5~CO.6', '2. Separated Boxes': 'GMD.4~CO.5~CO.6', '3. Rotate a Pyramid': 'GMD.4~CO.5~CO.6', '4. Match Silhouettes': 'GMD.4~CO.5~CO.6', '5. Removing Objects': 'GMD.4~CO.5~CO.6', '6. Stretch a Ramp': 'GMD.4~CO.5~CO.6', '7. Max 2 Boxes': 'GMD.4~CO.5~CO.6', '8. Combine 2 Ramps': 'GMD.4~CO.5~CO.6', '9. Scaling Round Objects': 'GMD.4~CO.5~CO.6',
               'Square Cross-Sections': 'GMD.4~CO.5~CO.6', 'Bird Fez': 'MG.1~GMD.4~CO.5~CO.6' , 'Pi Henge': 'MG.1~GMD.4~CO.5~CO.6', '45-Degree Rotations': 'GMD.4~CO.5~CO.6',  'Pyramids are Strange': 'GMD.4~CO.5~CO.6', 'Boxes Obscure Spheres': 'GMD.4~CO.5~CO.6', 'Object Limits': 'GMD.4~CO.5~CO.6', 'Tetromino': 'GMD.4~CO.5~CO.6', 'Angled Silhouette': 'GMD.4~CO.5~CO.6',
               'Warm Up':'GMD.4~CO.5~CO.6','Sugar Cones': 'GMD.4~CO.5~CO.6', 'Stranger Shapes': 'GMD.4~CO.5~CO.6', 'Tall and Small': 'GMD.4~CO.5~CO.6', 'Ramp Up and Can It': 'GMD.4~CO.5~CO.6', 'More Than Meets Your Eye': 'GMD.4~CO.5~CO.6', 'Not Bird': 'GMD.4~CO.5~CO.6', 'Unnecessary': 'GMD.4~CO.5~CO.6', 'Zzz': 'GMD.4~CO.5~CO.6', 'Bull Market': 'MG.1~GMD.4~CO.5~CO.6', 'Few Clues': 'GMD.4~CO.5~CO.6', 'Orange Dance': 'GMD.4~CO.5~CO.6', 'Bear Market': 'GMD.4~CO.5~CO.6'}



# Dictionary with the ELO difficulty of every puzzle
difficulty_puzzles = pd.read_csv('E:/Documentos/PCEO/5/Informatica/TFG/scripts/TFG-Informatica/Outputs/elo-puzzle-Output.csv', sep=";")

elo_puzzles_dict = {}
for index, row in difficulty_puzzles.iterrows():
    task_id = row['task_id']
    kc = row['kc']
    difficulty = row['difficulty']
    if task_id not in elo_puzzles_dict:
        elo_puzzles_dict[task_id] = {}
    elo_puzzles_dict[task_id][kc] = difficulty


def adaptedData(dataEvents, group = 'all'):

    dataEvents['time'] = pd.to_datetime(dataEvents['time'],utc=True)
    dataEvents = dataEvents.sort_values('time')

    #iterates in the groups and users of the data
    dataEvents['group'] = [json.loads(x)['group'] if 'group' in json.loads(x).keys() else '' for x in dataEvents['data']]
    dataEvents['user'] = [json.loads(x)['user'] if 'user' in json.loads(x).keys() else '' for x in dataEvents['data']]
    dataEvents['task_id'] = [json.loads(x)['task_id'] if 'task_id' in json.loads(x).keys() else '' for x in dataEvents['data']]

    # removing those rows where we dont have a group and a user that is not guest
    dataEvents = dataEvents[((dataEvents['group'] != '') & (dataEvents['user'] != '') & (dataEvents['user'] != 'guest'))]
    dataEvents['group_user_id'] = dataEvents['group'] + '~' + dataEvents['user']
    dataEvents['group_user_task_id'] = dataEvents['group'] + '~' + dataEvents['user']+'~'+dataEvents['task_id']




    # filtering to only take the group passed as argument

    activity_by_user = dataEvents.groupby(['group_user_id']).agg({'id':'count',
                                                                  'type':'nunique'}).reset_index().rename(columns={'id':'events',
                                                                                                                   'type':'different_events'})



    #initialize the metrics
    activity_by_user['active_time'] = np.nan
    activity_by_user['n_completed'] = 0
    activity_by_user['kc'] = ''
    #initialize the data structures
    puzzleEvents = dict()
    timePuzzle = dict()
    puzzCom= dict()
    puzzDestr = dict()
    initialTime = dict()

    n_attempts = dict()
    attData = dict()

    userPuzzleInit = dict()
    n_attemptsAux = dict()

    userTrain = set()
    userTest = set()
    userTotal = set()


    for user in dataEvents['group_user_id'].unique():

        # Computing active time
        previousEvent = None
        theresHoldActivity = 60 # np.percentile(allDifferences, 98) is 10 seconds
        activeTime = []

        user_events = dataEvents[dataEvents['group_user_id'] == user]
        user_puzzle_key = None

        for enum, event in user_events.iterrows():

            if(event['type'] in ['ws-start_level', 'ws-puzzle_started']):

                if(json.loads(event['data'])['task_id'] == 'Sandbox'): continue

                partialKey = event['group'] + '~' + event['user'] + '~' + json.loads(event['data'])['task_id']

                if(event['user'] not in userTotal):
                    userTotal.add(event['user'])


                if(partialKey not in n_attemptsAux.keys()):
                    n_attemptsAux[partialKey] = 0
                    puzzCom[partialKey] = 0


                if(partialKey not in userPuzzleInit.keys()):

                    n_attempts[partialKey] = 1
                    user_puzzle_key = event['group'] + '~' + event['user'] + '~' + json.loads(event['data'])['task_id'] + '~' + str(n_attempts[partialKey])
                    userPuzzleInit[partialKey] = 1

                else:

                    n_attempts[partialKey] += 1
                    user_puzzle_key = event['group'] + '~' + event['user'] + '~' + json.loads(event['data'])['task_id'] + '~' + str(n_attempts[partialKey])


                # initialize if the id is new
                if(user_puzzle_key not in puzzleEvents.keys()):
                    attData[user_puzzle_key] = {'att': 0, 'completed': 0,'dataCompleted': 0, 'accept': 0, 'timestamp': event['time'], 'repeat':0}
                    puzzleEvents[user_puzzle_key]= 1
                    timePuzzle[user_puzzle_key] = 0
                    puzzDestr[user_puzzle_key] = ''
                    initialTime[user_puzzle_key] = 0


                if(event['type'] in ['ws-puzzle_started']):
                    attData[user_puzzle_key]['timestamp'] = event['time']

            # the event is not final event
            if(event['type'] not in ['ws-exit_to_menu', 'ws-puzzle_complete', 'ws-create_user', 'ws-login_user']):
                if(user_puzzle_key in puzzleEvents.keys()):
                    puzzleEvents[user_puzzle_key] += 1
                    splitDes = user_puzzle_key.split("~")
                    puzzDestr[user_puzzle_key] = typeMappingKC[splitDes[2]]
                    if(event['type'] == 'ws-check_solution'):
                        attData[user_puzzle_key]['accept'] = 1




            # the puzzle ends
            if(event['type'] in ['ws-exit_to_menu', 'ws-puzzle_complete', 'ws-disconnect']):

                if(user_puzzle_key in puzzleEvents.keys()):
                    #the data is consistent
                    attData[user_puzzle_key]['dataCompleted'] += 1
                    #the data is valid
                    if(attData[user_puzzle_key]['accept'] == 1 and attData[user_puzzle_key]['dataCompleted']==1):
                        n_attemptsAux[partialKey]+=1
                        attData[user_puzzle_key]['att'] = n_attemptsAux[partialKey]
                        #attempt after solving
                        if(event['type'] in ['ws-puzzle_complete']):
                            if(puzzCom[partialKey] !=0 and n_attemptsAux[partialKey] > 1):
                                attData[user_puzzle_key]['repeat'] = 1

                    if(event['type'] in ['ws-puzzle_complete']):
                        if(puzzCom[partialKey] ==0):
                            attData[user_puzzle_key]['completed'] = 1
                            if(attData[user_puzzle_key]['accept'] == 1):
                                puzzCom[partialKey] +=1




    # add the data by group_user_task_id
    for i in attData.keys():
        key_split = i.split('~')

        if(len(userTrain) < round(len(userTotal)*0.7)):
            userTrain.add(key_split[1])
        else:
            if(key_split[1] not in userTrain): userTest.add(key_split[1])



        if(key_split[2] != '' and key_split[2] != 'Sandbox' and key_split[3] != '' and i != '' and key_split[1] != ''):
            if(attData[i]['accept'] != 0 and attData[i]['dataCompleted'] != 0 and attData[i]['repeat'] == 0):

                # data output preparation
                activity_by_user.at[i, 'group_user_task_att'] = key_split[0] + '~' + key_split[1] + '~' + key_split[2] + '~' + str(attData[i]['att'])
                activity_by_user.at[i, 'group'] = key_split[0]
                activity_by_user.at[i, 'user'] = key_split[1]
                activity_by_user.at[i, 'task_id'] = key_split[2]
                activity_by_user.at[i, 'attempt'] = attData[i]['att']
                activity_by_user.at[i, 'repeat'] = attData[i]['repeat']
                activity_by_user.at[i, 'kc'] = puzzDestr[i]
                activity_by_user.at[i, 'n_completed'] = attData[i]['completed']
                activity_by_user.at[i, 'initial timestamp'] = attData[i]['timestamp']



    #delete row with NaN
    if 'user' in activity_by_user.columns:
        activity_by_user.dropna(subset = ['user'], inplace=True)
        #data output preparation
        activity_by_user = pd.DataFrame(activity_by_user, columns = ['group_user_task_att', 'group','user','task_id','n_completed', 'kc', 'initial timestamp'])





    return activity_by_user


# Dict users: uDict
def usersDict(datafile):
    csv_file = datafile
    mapUsers = {}
    mapGroups = {}
    cont =0
    for row in csv_file.iterrows():
        user = row[1]['user']
        group = row[1]['group']
        if user not in mapUsers.keys():
            mapUsers[user]=cont
            mapGroups[user] = group
            cont = cont+1
    return mapUsers, mapGroups


# Dict puzzles: qDict
def puzzlesDict(datafile):
    csv_file = datafile
    mapPuzzles = {}
    cont =0
    for row in csv_file.iterrows():
        question = row[1]['task_id']
        if question not in mapPuzzles.keys():
            mapPuzzles[question]=cont
            cont = cont+1
    return mapPuzzles



# Dict kcs: kcDict
def kcsDict(datafile):
    QT = []
    csv_file = datafile
    mapKc = {}
    cont =0
    for row in csv_file.iterrows():
        tags = row[1]['kc']
        if tags:
            tag = tags.split("~")
            for topics in tag:
                if topics not in mapKc.keys():
                    mapKc[topics]=cont
                    cont = cont + 1
    return mapKc

def createKcDict(datafile):
    
    QTMat = dict()
    csv_file = datafile
    for row in csv_file.iterrows():
        qid = row[1]['task_id']
        kcs = row[1]['kc']
        if(qid not in QTMat.keys()):
            QTMat[qid]=dict()
        if kcs:
            kc = kcs.split("~")
            for k in kc:
                QTMat[qid][k] =0


    for puzzle in QTMat.keys():
        tam = len(QTMat[puzzle])
        if tam>0:
            if(puzzle in mg1Puzzles):
                QTMat[puzzle]['MG.1'] = 0.5
                for x in QTMat[puzzle].keys():
                    if(x != 'MG.1'):
                        QTMat[puzzle][x] = 0.5/(tam-1)
            elif(puzzle in gmd4Puzzles):
                QTMat[puzzle]['GMD.4'] = 0.5
                for x in QTMat[puzzle].keys():
                    if(x != 'GMD.4'):
                        QTMat[puzzle][x] = 0.5/(tam-1)
            elif(puzzle in co5Puzzles):
                QTMat[puzzle]['CO.5'] = 0.5
                for x in QTMat[puzzle].keys():
                    if(x != 'CO.5'):
                        QTMat[puzzle][x] = 0.5/(tam-1)
            elif(puzzle in co6Puzzles):
                QTMat[puzzle]['CO.6'] = 0.5
                for x in QTMat[puzzle].keys():
                    if(x != 'CO.6'):
                        QTMat[puzzle][x] = 0.5/(tam-1)
            else:
                for x in QTMat[puzzle].keys():
                    QTMat[puzzle][x] = 1/tam
    return QTMat


def loadDataset(datafile):
    uDict, gDict = usersDict(datafile)
    qDict =puzzlesDict(datafile)
    #kcDict =kcsDict(datafile)
    kcsPuzzleDict =  createKcDict(datafile)

    return uDict, gDict,qDict, kcsPuzzleDict



# ELO algorithm with static difficulty
def multiTopic_ELO(inputData, Competency, Diff, A_count, Q_count, kcsPuzzleDict ,gDict,gamma, beta):

    alpha = 1
    alpha_denominator = 0
    correct = 0
    prob_test = dict()
    ans_test = dict()
    probUser = dict()
    competencyPartial = dict()
    userPuzzles = dict()
    completedPartialData = dict()
    
    failAtt = dict()
    
    probUserTest = dict()
    ansUserTest = dict()
    
    contPuzzlesUser = dict()

    response = np.zeros((len(inputData), 1))
    
    for count, (index, item) in enumerate(inputData.iterrows()):
            
        alpha_denominator = 0
        uid = item[student_id]
        qid = item[puzzle_name]
        time = item[timestamp]
        
        if(uid not in failAtt.keys()):
            failAtt[uid]= dict()
        if(qid not in failAtt[uid].keys()):
            failAtt[uid][qid] = 0
        
        if(uid not in userPuzzles.keys()): userPuzzles[uid] = []
        userPuzzles[uid].append(qid)
        
        # Cont the puzzles per user (intermediate and advanced)
        if(uid not in contPuzzlesUser.keys()):
            contPuzzlesUser[uid] = set()
        if(qid in intermediatePuzzles or qid in advancedPuzzles):
            contPuzzlesUser[uid].add(qid)
        
        diff = dict()
        diff[qid]=[]
        comp= dict()
        comp[uid]=[]
        
        # The student's current competence by component is multiplied by each component of the question he or she is facing.
        for k in kcsPuzzleDict[qid]:
            comp[uid].append(Competency[uid][k] * kcsPuzzleDict[qid][k])
            diff[qid].append(Diff[qid][k] * kcsPuzzleDict[qid][k])
            
        # Adding up the competencies per component to obtain the global competence
        compTotal = np.sum(comp[uid])
        diffTotal = np.sum(diff[qid])
        
        # With the global competition and the difficulty of the question, the probability of solving it is calculated
        probability = (1)/(1 + math.exp( -1 * (compTotal - diffTotal)))
        
        if(uid not in prob_test.keys()):
            prob_test[uid] = dict()
            
        if(uid not in probUserTest.keys()):
            probUserTest[uid] = []
            
        if(uid not in ansUserTest.keys()):
            ansUserTest[uid] = []
        
        # Save the probabilities
        prob_test[uid][qid]=probability
        q_answered_count = Q_count[qid]
        
        if(qid in intermediatePuzzles or qid in advancedPuzzles):
            probUserTest[uid].append(probability)
        
        # The puzzle is completed or no
        if item[completed] == 1:

            response[count] = 1
            correct = 1
        else:
            response[count] = 0
            correct = 0
            failAtt[uid][qid] +=1
        
        if(uid not in ans_test.keys()):
            ans_test[uid] = dict()
            
        # Save the real result
        ans_test[uid][qid] = correct
        if(qid in intermediatePuzzles or qid in advancedPuzzles):
            ansUserTest[uid].append(correct)

        #Alpha component is calculated (normalization factor)
        alpha_numerator = probability - correct
        for k in kcsPuzzleDict[qid]:
            c_lambda = Competency[uid][k]
            probability_lambda = (1)/(1 + math.exp( -1 * (c_lambda - Diff[qid][k])))
            alpha_denominator = alpha_denominator + (correct - probability_lambda)
        alpha = abs(alpha_numerator / alpha_denominator)

        # Initialize new data
        if(uid not in probUser.keys()):
            probUser[uid] = dict()
            competencyPartial[uid] = dict()
        
        probUser[uid][qid]= probability
        
        Q_count[qid] += 1
        A_count[uid] += 1
        for k in kcsPuzzleDict[qid]:
            
            u_answered_count = A_count[uid]
            c = Competency[uid][k]
            prevDiff = Diff[qid][k]
            
            key = uid+'~'+qid+'~'+k+'~'+str(round(Competency[uid][k],3)) + '~'+str(round(prevDiff,3))
            
            # Competency probability is calculated
            probability = (1)/(1 + math.exp( -1 * (Competency[uid][k] - prevDiff)))
            
            # Update the difficulty
            #changeDiff = ((gamma)/(1 + beta * q_answered_count)) *alpha* (probability - correct)
            #Diff[qid][k] = Diff[qid][k] + kcsPuzzleDict[qid][k] * changeDiff
            
            # Update the competency
            # if puzzle is in tutorial puzzles, we do not update the competency
            weightAtt = 0
            if(correct ==1):
                # Fail limit
                if(failAtt[uid][qid] >= 5): failAtt[uid][qid] == 5
                    
                weightAtt = (1-(failAtt[uid][qid]/10))
                complete_change = kcsPuzzleDict[qid][k] * (gamma)/(1 + beta * u_answered_count) * alpha * (correct - probability)
                changeComp = kcsPuzzleDict[qid][k] * (gamma)/(1 + beta * u_answered_count) * alpha * (correct - probability) * weightAtt
                Competency[uid][k] = Competency[uid][k]+changeComp
                
            else:
                
                changeComp = 0
                complete_change = 0
                
            # Save the new data
            completedPartialData[key] = {'prob': 0, 'kcs importance': 0, 'correct': -1, 'Difficulty': 0, 'Group Difficulty': 0, 'update competency': 0}
            completedPartialData[key]['prob'] = probability
            completedPartialData[key]['kcs importance'] = kcsPuzzleDict[qid][k]
            completedPartialData[key]['correct'] = correct
            completedPartialData[key]['Difficulty'] = round(Diff[qid][k],3)
            completedPartialData[key]['Weight'] = weightAtt
            completedPartialData[key]['cont_puzzles'] = len(contPuzzlesUser[uid])
            completedPartialData[key]['timestamp'] = time
            completedPartialData[key]['changeComp'] = changeComp
            completedPartialData[key]['complete_change_comp'] = complete_change
            #completedPartialData[key]['changeDiff'] = kcsPuzzleDict[qid][k] * changeDiff
            
            if(k not in competencyPartial[uid].keys()): competencyPartial[uid][k] = []
            competencyPartial[uid][k].append(Competency[uid][k])
            
                
    return Competency, A_count , Q_count, prob_test, ans_test, competencyPartial, probUser, userPuzzles, completedPartialData, probUserTest, ansUserTest, contPuzzlesUser


def run(gamma, beta, totalData, user_objective = 'all', group_objective = 'all', puzzle_objective = 'all'):

    uDict,gDict,qDict,kcsPuzzleDict = loadDataset(totalData)
    competency_ELO = pd.DataFrame()


    # Data for step by step data output
    question_counter = dict()

    for q in qDict.keys():
        if(q not in question_counter.keys()):
            question_counter[q]=dict()
            question_counter[q]=0

    learner_competency = dict()
    response_counter = dict()
    for user in uDict.keys():
        if(user not in learner_competency.keys()):
            learner_competency[user]=dict()
            response_counter[user]=dict()
            response_counter[user]=0
        for k in ['GMD.4','CO.5','CO.6','MG.1']:
            learner_competency[user][k]=0

    # Multi-ELO function
    learner_competency_total, response_counter_total, question_counter_total, prob_total, ans_total, competencyPartial_total, probUser_total, userPuzzles_total, completedPartialData, probUserTest, ansUserTest, contPuzzlesUser = multiTopic_ELO(totalData, learner_competency, elo_puzzles_dict, response_counter, question_counter, kcsPuzzleDict,gDict,gamma, beta)

    elo = 0
    for key in learner_competency[user_objective].keys():
        elo += learner_competency[user_objective][key]
        
    return round(elo,2)

