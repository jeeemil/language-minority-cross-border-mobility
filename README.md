# Master's thesis repo of Emil Ehnström

This repository contains scripts that has been used in the master's thesis of Emil Ehnström.

This repository is originally forked from the [Digital Geography Lab's borderspace repository] (https://github.com/DigitalGeographyLab/borderspace-tools)
The scripts have then been modified for the thesis and scripts from the original repository, that have not been used in the thesis, have been excluded.

The workflow of the thesis follows:

1. Residents of Finland were identified by the [twitter-home-detector-tool](https://github.com/DigitalGeographyLab/twitter-home-detector-tool). With **get_fin_residents.py** and  **all_tweets_from_fin_users.py** I collect all the data for the user and uploading a new table to a PostgrSQL server.
2. I run the script **multilang_detection.py** that is sligthly modified version of [twitter_multilangid.py](https://github.com/DigitalGeographyLab/maphel-finlang/blob/master/get_user_langprofiles.py) by Tuomas Väisänen and Tuomo Hiippala. The script returns a PostgreSQL database. 
3. I use the script **lang_profile.py** that is based upon **[this script](https://github.com/DigitalGeographyLab/maphel-finlang/blob/master/get_user_langprofiles.py)** by Tuomas Väisänen. It reads the database of tweets with and identified language and returns a language profile of the user to a PostgreSQL database. I also use the **get_lang_summary.py** for analysing the amount of users in each language groups.
4. I run the multiprocessing script **line.py** to create the movement for each language group. The script is based upon works of Håvard Aagesen and Tuomas Väisänen. The lines are later visualised in QGIS.
5. I run a similar script called **municipality.py** to create movement lines within Finland. 
6. To deterimne the diversity I use **get_richness_ecopy.py** and calculate the Shannon entropy and Simpson index of the unique country travel diversity. 
7. For statistics I use **cb_summary.py**, **continents_summary.py**, **municipality_for_excel.py**, and **municipality_summary** together with Excel. 
8. For further analyses in SPSS I use **group_comparison.py** to get a proper .csv file. 
9. Some additional scripts were used for plottin, table writing and other minor operations. 