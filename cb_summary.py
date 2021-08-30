"""Small script to calcluate descriptive statistics"""
import pandas as pd

lang = 'et'
#read the pickle file

data = pd.read_pickle(f'{lang}_all_cb_movements.pkl')

print('Number of cross-border visits: ' + str(len(data)))
print('Cross-border movements per user: ' + str(len(data)/len(data.user_id.unique())))
print('Number of users: ' + str(len(data.user_id.unique())))

