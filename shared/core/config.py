# SET UP PYTHON VARIABLES


THE_SHOW_YEARS = [25, 24, 23, 22, 21]
CURRENT_SHOW_YEAR = 25

MAJOR_ROSTER_UPDATES = {
    25: [5, 8, 11, 16, 19, 23, 27, 28, 29],
    24: [4, 7, 10, 13, 15, 18, 22, 23],
    23: [1, 4, 7, 11, 14, 17, 21, 25, 26], # Don't use 15 - it is incorrect (duplicated with 11)
    22: [1, 3, 5, 6, 8, 11, 12, 15, 17, 19, 21],
    21: [1, 2, 3, 4, 6, 8, 10, 13, 15, 18, 20, 21]  # Don't use 11 - it is incorrect (duplicate of 10)
}

FIELDING_ROSTER_UPDATES = {
    25: [19],
    24: [15],
    23: [14, 25],
    22: [12],
    21: [10]
}