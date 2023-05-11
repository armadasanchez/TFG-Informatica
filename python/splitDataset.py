from sklearn.model_selection import train_test_split

def splitDataset(dataFeatures):
    # i get the list of users
    users = dataFeatures['user'].unique()

    # split the list of users in train and test
    users_train, users_test = train_test_split(users, test_size=0.3, random_state=33)

    # Get the rows from the users in the train set
    dataFeatures_train = dataFeatures[dataFeatures['user'].isin(users_train)]
    dataFeatures_test = dataFeatures[dataFeatures['user'].isin(users_test)]

    # Split the train and test set in x and y
    dataFeatures_train_y = dataFeatures_train.completed
    dataFeatures_test_y = dataFeatures_test.completed

    dataFeatures_test_x = dataFeatures_test.drop(['timestamp','cum_this_puzzle_attempt','n_attempt','group', 'user', 'task_id','completed'], axis=1)
    dataFeatures_train_x = dataFeatures_train.drop(['timestamp','cum_this_puzzle_attempt','n_attempt','group', 'user', 'task_id','completed'], axis=1)

    # Return the train and test set
    return dataFeatures_train_x, dataFeatures_test_x, dataFeatures_train_y, dataFeatures_test_y
