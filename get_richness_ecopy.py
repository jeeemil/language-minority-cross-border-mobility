from matplotlib import use
from scipy.special import comb
import scipy.misc
scipy.misc.comb = comb
import ecopy as ep
import pandas as pd

langs = ['et', 'ru', 'sv', 'fi']

for lang in langs:
    #read the pickle file
    data = pd.read_pickle(f'{lang}_all_cb_movements.pkl')


    # Creating a set with all unique countries in the data
    countries = set(data.dest_country.unique())
    countries2 = set(data.orig_country.unique())
    all_countries = countries|countries2
    all_countries = list(all_countries)
    new_df = pd.DataFrame(columns=all_countries, index=data.user_id.unique())
    new_df = new_df.fillna(0)

    # Grouping data by user
    grouped_users = data.groupby('user_id')

    for key, group in grouped_users:
        user_country_counts = pd.DataFrame(columns=['country', 'counts'])
        dests = group['dest_country'].value_counts().rename_axis('country').reset_index(name='counts')
        origs = group['orig_country'].value_counts().rename_axis('country').reset_index(name='counts')
        user_country_counts = dests.append(origs).reset_index()
        user_agg_df = user_country_counts.groupby('country').agg('sum').reset_index()
    
        user_agg_df = user_agg_df.loc[user_agg_df['country']!= 'Finland']
        user_agg_df  = user_agg_df.set_index('country')
        
        for i, row in user_agg_df.iterrows():
            new_df.loc[key, i] = row['counts']

    shannon = ep.diversity(new_df, 'shannon')
    simpson = ep.diversity(new_df, 'simpson')

    new_df['shannon'] = shannon
    new_df['simpson'] = simpson
    new_df['user_id'] = new_df.index
    new_df['shannon'] = new_df['shannon'].round(4)
    new_df['simpson'] = new_df['simpson'].round(4)
    new_df[['shannon', 'simpson', 'user_id']].to_csv(f'outputs/user with shannon/{lang}_index_table_new.csv')
