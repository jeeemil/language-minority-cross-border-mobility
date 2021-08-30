"""This script is used for plotting boxplots of different variables"""
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.lines as mlines


plt.rcParams["font.family"] = "Times New Roman"
data = pd.read_pickle(r'outputs/group_comparison.pkl')
data = pd.read_pickle('outputs/all_lang_share.pkl')
lang = 'ru'
data = pd.read_pickle(f'outputs/correlation/{lang}_corr.pkl')
"""column1 = 'lang_cb_share'
title = 'Share of trips to country with majority language'

ax = data.boxplot(column=column1, showfliers=False, meanline=True, showmeans=True, meanprops={'color':'red'})
plt.title(title)
plt.xticks([1,],['Swedish'])

mean_line = mlines.Line2D([], [], color='red', linestyle='--',
                          markersize=15, label='Mean')
median_line = mlines.Line2D([],[], color='green',
                          markersize=15, label='Median')
plt.legend(handles=[mean_line, median_line])
fig = ax.get_figure()
fig.suptitle('')
ax.set(xlabel=None)
plt.show()
"""
print(f"{lang} Mean share: {sum(data['lang_cb_share'])/len(data)}")

data['lang_code'] = data['lang']
data['lang_code'] = data['lang_code'].replace({'et':1, 'ru':2, 'sv':3, 'fi':4})

column1 = 'simpson'
title = 'Simpson index'

ax = data.boxplot(column=column1, by='lang_code', showfliers=False, meanline=True, showmeans=True, meanprops={'color':'red'})
plt.title(title)
plt.xticks([1,2,3,4],['Estonian', 'Russian', 'Swedish', 'Finnish'])

mean_line = mlines.Line2D([], [], color='red', linestyle='--',
                          markersize=15, label='Mean')
median_line = mlines.Line2D([],[], color='green',
                          markersize=15, label='Median')
plt.legend(handles=[mean_line, median_line], loc='upper left')
fig = ax.get_figure()
fig.suptitle('')
ax.set(xlabel=None)
plt.show()

#plt.savefig(f'outputs/boxplots/no outliers/boxplot_{column1}_no_outliers.png')"""

print(data.lang_cb_share.max())
grouped = data.groupby('lang')
print(len(data))
for key, group in grouped:
    print(f"{key} Mean share: {sum(group['lang_cb_share'])/len(group)}")


"""langs = ['et', 'ru', 'sv', 'fi']

data = pd.read_pickle(f'{lang}_all_cb_movements.pkl')"""
