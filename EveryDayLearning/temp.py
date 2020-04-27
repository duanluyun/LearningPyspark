import numpy as np
import xlrd

## Read the data of user rates
def ReadData():
    data = xlrd.open_workbook("./IntelligenceRecommendation/u_data.xls")
    dataTable = data.sheet_by_name(u'Sheet1')
    userNum = 943
    movieNum = 1682
    nrows = dataTable.nrows
    ncols = dataTable.ncols
    allUserRates = np.zeros([userNum + 1, movieNum + 1])
    for i in range(1, nrows):
        allUserRates[int(dataTable.row_values(i)[0]), int(dataTable.row_values(i)[1])] = dataTable.row_values(i)[2]
        allUserRates[int(dataTable.row_values(i)[3]), int(dataTable.row_values(i)[4])] = dataTable.row_values(i)[5]
    return allUserRates

## Calculate the consine similarity
def CalSimilar(A, B):
    inA = np.mat(A)
    inB = np.mat(B)
    num = float(inA * inB.T)
    denom = np.linalg.norm(inA) * np.linalg.norm(inB)
    cos = num / denom
    # sim = 0.5 + 0.5 * cos
    return cos

## Find the neighbors of the target user
def FindNeighbor(targetUser, allUserRates, neighborNum):
    userNum = allUserRates.shape[0]
    simScor = np.zeros(userNum)
    targetSimUsersInfo = []
    for j in range(userNum):
        simScor[j] = CalSimilar(allUserRates[targetUser, :], allUserRates[j, :])
    print(simScor)
    targetSimUsersInfo.append(list(np.argsort(-simScor)[1:neighborNum+1]))
    targetSimUsersInfo.append(list(simScor[np.argsort(-simScor)][1:neighborNum+1]))
    return targetSimUsersInfo



## Generate top-N recommendation for the target user
def GenerateRecommand(targetUser, allUserRates, neighborInfo, RecomNum):
    MovieUsers = np.transpose(allUserRates)
    neighborNum = len(neighborInfo)
    movieNum = MovieUsers.shape[0]
    cursum1, cursum2 = 0 , 0
    predictScores = np.zeros(movieNum)
    for i in range(1, movieNum):
        for j in range(neighborNum):
            if MovieUsers[i][int(neighborInfo[0][j])]:
                cursum1 += MovieUsers[i][neighborInfo[0][j]] * neighborInfo[1][j]
                cursum2 += neighborInfo[1][j]
        if cursum2:
            predictScores[i] = cursum1 / cursum2
    sortedMovies = np.argsort(-predictScores)
    ## delete the items that the user has rated
    recomMovies = []
    count = 0
    for movie in sortedMovies:
        if not allUserRates[targetUser][movie]:
            recomMovies.append(movie)
            count += 1
        if count == RecomNum:
            break
    return recomMovies

if  __name__ == "__main__":
    targetUser = 0
    rates = np.array([[4,3,0,0,5,0],[5,0,4,0,4,0],[4,0,5,3,4,0],[0,3,0,0,0,5],[0,4,0,0,0,4],[0,0,2,2,0,5]])
    neighborNum = 2
    neighbor = FindNeighbor(targetUser, rates, neighborNum)
    print(neighbor)
    # recomMovies = GenerateRecommand(targetUser, rates, neighbor, 2)
    # print (recomMovies)