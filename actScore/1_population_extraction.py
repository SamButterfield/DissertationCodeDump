# -*- coding: utf-8 -*-
"""
@purpose Extracting the Population that exist in GP Clinical and have activity timestamps
@author: Sam Butterfield (UG)
"""

import pandas as pd
import numpy as np

print("Start Extracting Population")

# ============================== Import Datasets ==============================
gpc_df = pd.read_csv('/nobackup/proj/pmp4nu/UKBB_FULL_DATAFRAME/HEALTH_OUTCOMES/PRIMARY_CARE/gp_clinical.txt', sep = '\t', encoding = 'unicode_escape')
activity_df = pd.read_csv('activityTimestamps.csv', sep = ',')
print("data imported")


# ============================== Clean GP Clinical ==============================
# replace nan in event_dt column
gpc_df['event_dt'] = gpc_df['event_dt'].fillna('01/01/1920')
# replace rest of NaN's with 0
#gpc_df = gpc_df.fillna(0)

# DROP DUPLICATES
print("Duplicates Dropped")
#print("Duplicates NOT Dropped")
gpc_df.drop_duplicates(inplace = True)


# ============================== Filter to get individuals who exist in all datasets ==============================
# gp_clinical only take eid's that exist in activity_df
gpc_df = gpc_df[(gpc_df.eid.isin(activity_df.eid))]
print("Population Intersected")

# ============================== import Baseline to join to datasets ==============================
base_df = pd.read_csv('/nobackup/proj/pmp4nu/UKBB_FULL_DATAFRAME/ukbiobank_baseline-data_SC_v22.csv', sep=',')
# Take only eid and health flag
base_df = base_df.loc[:,base_df.columns.isin(['eid', 'HEALTHY_CVD_T2D1_T2D2'])]
#join to gp_clinical
gpc_df = pd.merge(gpc_df, base_df, on = 'eid', how = 'inner')
print("baseline health classification added")


# ============================== Mapping Read 3 to Read 2 ==============================
# Import to mapping df
r3_map = pd.read_csv('read_3_read_2_mapping.csv', sep = ',', dtype = str)
# Setup mapping file
r3_map = dict(r3_map[['READV3_CODE','READV2_CODE']].values)
# adding a mapping for 0 as I will replace na with 0, therefor all values shoulld exist 
r3_map[0] = "NO_MAPPING"

# Map no exaustingly
# What Collumn to map, collumn to take the key from, which column to replace with the key column for that row
gpc_df['read_2'] = gpc_df['read_3'].map(r3_map).fillna(gpc_df['read_2'])
print("Mapped Read 3 to Read 2")

# ============================== write to CSV ==============================
print("write to csv uncommented")
gpc_df.to_csv(path_or_buf = "1_gpc_intersected.csv" , index = False)





