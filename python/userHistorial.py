import json

typeMapping = ['Sandbox~SAND', '1. One Box~Tutorial', '2. Separated Boxes~Tutorial', '3. Rotate a Pyramid~Tutorial', '4. Match Silhouettes~Tutorial', '5. Removing Objects~Tutorial', '6. Stretch a Ramp~Tutorial', '7. Max 2 Boxes~Tutorial', '8. Combine 2 Ramps~Tutorial', '9. Scaling Round Objects~Tutorial',
               'Square Cross-Sections~Easy Puzzles', 'Bird Fez~Easy Puzzles', 'Pi Henge~Easy Puzzles', '45-Degree Rotations~Easy Puzzles',  'Pyramids are Strange~Easy Puzzles', 'Boxes Obscure Spheres~Easy Puzzles', 'Object Limits~Easy Puzzles', 'Not Bird~Easy Puzzles', 'Angled Silhouette~Easy Puzzles',
               'Warm Up~Hard Puzzles','Tetromino~Hard Puzzles', 'Stranger Shapes~Hard Puzzles', 'Sugar Cones~Hard Puzzles', 'Tall and Small~Hard Puzzles', 'Ramp Up and Can It~Hard Puzzles', 'More Than Meets Your Eye~Hard Puzzles', 'Unnecessary~Hard Puzzles', 'Zzz~Hard Puzzles', 'Bull Market~Hard Puzzles', 'Few Clues~Hard Puzzles', 'Orange Dance~Hard Puzzles', 'Bear Market~Hard Puzzles']

tutorialPuzzles = []
for puzzle in typeMapping:
    desc = puzzle.split("~")
    if(desc[1] == 'Tutorial'):
        tutorialPuzzles.append(desc[0])


intermediatePuzzles = []
for puzzle in typeMapping:
    desc = puzzle.split("~")
    if(desc[1] == 'Easy Puzzles'):
        intermediatePuzzles.append(desc[0])

advancedPuzzles = []
for puzzle in typeMapping:
    desc = puzzle.split("~")
    if(desc[1] == 'Hard Puzzles'):
        advancedPuzzles.append(desc[0])


def computeUserHistorial(dataEvents, user):

    completed = dict()
    tutorialCompleted = dict()
    intermediateCompleted = dict()
    advancedCompleted = dict()
    userAttempts = dict()
    userWins = dict()

    tutorialCompleted[user] = 0
    intermediateCompleted[user] = 0
    advancedCompleted[user] = 0
    userAttempts[user] = 0
    userWins[user] = 0

    puzzle_key = None

    for enum, event in dataEvents.iterrows():

        if event['user'] == user:

            if(event['type'] in ['ws-start_level', 'ws-puzzle_started']):

                puzzle_key =  json.loads(event['data'])['task_id']
                if(puzzle_key not in completed.keys()): completed[puzzle_key] = 0

            if(event['type'] == 'ws-puzzle_started' and completed[puzzle_key] == 0):

                userAttempts[user] +=1

            elif(event['type'] == 'ws-puzzle_complete' and completed[puzzle_key] == 0):

                userWins[user] += 1

                if (puzzle_key in tutorialPuzzles):
                    tutorialCompleted[user] += 1

                elif (puzzle_key in intermediatePuzzles):
                    intermediateCompleted[user] += 1

                elif (puzzle_key in advancedPuzzles):
                    advancedCompleted[user] += 1

                completed[puzzle_key] = 1

    resultado = dict()

    if userAttempts[user] == 0 or userWins[user] == 0:
        attempts_per_puzzle = 0
    else: attempts_per_puzzle = round((userAttempts[user]/userWins[user]),2)

    resultado['percentage_tutorial'] = round((tutorialCompleted[user]/9)*100,2)
    resultado['percentage_intermediate'] = round((intermediateCompleted[user]/9)*100,2)
    resultado['percentage_advanced'] = round((advancedCompleted[user]/13)*100,2)
    resultado['attempts_per_puzzle'] = attempts_per_puzzle

    return resultado