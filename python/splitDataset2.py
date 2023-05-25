import numpy as np

def splitDataset2(dataFeatures):
    # i get the list of users
    users = dataFeatures['user'].unique()

    # divide the list of users in 10 different groups equally distributed
    users_groups = np.array_split(users, 10)

    # El data frame dataFeatures contiene una columna llamada user que contiene el id del usuario. Quiero añadir una nueva columna al dataframe que contenga el grupo al que pertenece el usuario.
    # Para ello, creo un diccionario que contenga como clave el id del usuario y como valor el grupo al que pertenece.
    # Después, creo una nueva columna en el dataframe que contenga el grupo al que pertenece el usuario.
    # Para ello, recorro el dataframe y para cada fila, obtengo el id del usuario y busco en el diccionario el grupo al que pertenece.
    # Finalmente, añado una nueva columna al dataframe que contenga el grupo al que pertenece el usuario.

    # Creo un diccionario que contenga como clave el id del usuario y como valor el grupo al que pertenece.
    user_group = {}

    # Recorro los grupos de usuarios
    for i in range(len(users_groups)):
        # Recorro los usuarios de cada grupo
        for user in users_groups[i]:
            # Añado al diccionario el usuario y el grupo al que pertenece
            user_group[user] = i

    dataFeatures['group'] = dataFeatures.apply(lambda row: user_group[row.user], axis=1)

    # quiero convertir la columna group en una lista para devolverla en la función
    groups = dataFeatures['group'].tolist()

    dataFeatures.drop(['group'], axis=1, inplace=True)

    return groups







