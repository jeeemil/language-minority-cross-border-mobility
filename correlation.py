import scipy.stats
import pandas as pd
import matplotlib.pyplot as plt
data = pd.read_pickle(r'outputs/group_comparison_w_share.pkl')
#data = pd.read_csv(r'outputs/group_comparison_w_share.csv')
data = data.dropna(axis=0)
print(data.dtypes)

column1 = 'lang_cb_share'
column2 = 'km_mean'
langs = ['et', 'ru', 'sv', 'fi']
#langs = [1, 2, 3, 4]

#data = data.loc[data['unique_countries']>1]
for lang in langs:

    lang_data = data.loc[data['lang'] == lang]
    #lang_data = data.loc[data['lang_code'] == lang]
    #lang_data = lang_data.dropna(axis=0)
    #lang_data = lang_data.astype({'unique_municipalities':int, 'unique_countries':int, 'shannon':float, 'simpson':float})
    #lang_data.to_excel(f'{lang}_test.xlsx')
    lang_data = lang_data.dropna()
    list1 = list(lang_data[column1])
    list2 = list(lang_data[column2])
    print(len(list1))
    print(len(list2))
    res = scipy.stats.linregress(list1, list2)
    
    line = f'Regression line: y={res.intercept:.2f}+{res.slope:.2f}x, r={res.rvalue:.2f}'
    print(line)
    fig, ax = plt.subplots(figsize=(10,10))
    ax.plot(list1, list2, linewidth=0, marker='s', label=f'r={res.rvalue:.2f}')
    ax.plot(list1, res.slope*(list1+res.intercept), label=line)
    ax.set_xlabel(column1)
    ax.set_ylabel(column2)

    #ax.set_ylim(1,20000)
    #ax.set_xlim(1,24000)
    fig.suptitle(f'{lang} Pearsons correlation, R = {res.rvalue:.2f}')
    print(f'{lang} Pearsons correlation, R = {res.rvalue:.2f}')
    ax.legend(facecolor='white')
    #plt.show()
    #fig.savefig(f'outputs/correlation/new correlation/{lang}_{column1}_{column2}_correlation.png', dpi=100)




