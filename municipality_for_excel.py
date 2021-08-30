import pandas as pd
from statistics import mean

langs = ['et', 'ru', 'sv', 'fi']
user_municipalities = pd.DataFrame(columns=['lang', 'unique_municipalities', 'km_mean'])

for lang in langs:
    data_m = pd.read_pickle(f'{lang}_municipality_lines.pkl')
    data_m = data_m.dropna()
    data_cb = pd.read_pickle(f'{lang}_all_cb_movements.pkl')
    
    user_list = set((data_cb['user_id'].astype(int)))

    data = data_m[data_m['user_id'].isin(user_list)]

    grouped_users = data.groupby('user_id')

    print(len(data.user_id.unique()))

    for key, group in grouped_users:

        municipalities = set(group.dest_mun.unique())
        municipalities2 = set(group.orig_mun.unique())
        all_municipalities = municipalities|municipalities2
        unique_municipalities = len(all_municipalities) - 1
        km_mean = mean(group['distance_km'])
        user_municipalities = user_municipalities.append({'lang':f'{lang}', 'unique_municipalities':unique_municipalities, 'km_mean': km_mean}, ignore_index=True)


#user_municipalities = user_municipalities.fillna(0)
user_municipalities['unique_municipalities'] = user_municipalities['unique_municipalities'].astype(int)
user_municipalities['lang_code'] = user_municipalities['lang']
user_municipalities['lang_code'].replace({'et':1, 'ru':2, 'sv':3, 'fi':4}, inplace=True)
#user_municipalities.to_csv(r'outputs/all_unique_municipalities.csv', sep=';')

bins = [0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 100, 200 ,300]

user_municipalities['binned'] = pd.cut(user_municipalities['unique_municipalities'], bins)

#user_municipalities_csv.to_csv(f'outputs/all_municipalities_bins.csv', sep=';')
grouped = user_municipalities.groupby('lang')
for key, group in grouped:
    group_csv = group['binned'].value_counts().sort_index().rename_axis('bins').to_frame('user count')
    group_csv.to_csv(f'outputs/{key}_municipality_bins.csv', sep=';')