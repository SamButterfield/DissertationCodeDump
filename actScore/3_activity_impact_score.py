# -*- coding: utf-8 -*-
"""
@purpose Extracting Chapters at desired byte depth
@author: Sam Butterfield (UG)
"""

import pandas as pd
import numpy as np

# ============================== Import Datasets ==============================
gpc_df = pd.read_csv('2_gpc_code_extraction_surrounding_timestamp.csv', sep = ',')
# ===== Import Code Impact Scoring =====
code_scoring = pd.read_csv('code_impact_scoring_edit_200415.csv', sep = ',', dtype = str)
code_scoring = code_scoring[['read_extract','impact_score']].copy()
print("data imported")

# ============================== Getting Score Format ==============================
gpc_score = gpc_df.groupby(['eid','read_extract']).count().reset_index()
gpc_score = gpc_score[['eid','read_extract','read_2']]
# Join to the data
gpc_score = pd.merge(gpc_score, code_scoring, on = 'read_extract',how = 'inner')

# ===== Calculating Score =====
num_eid = gpc_df.copy()
num_eid['eid_count'] = num_eid.groupby('eid')['eid'].transform('count')
num_eid = num_eid[['eid','eid_count']]
num_eid = num_eid.drop_duplicates(keep = 'first')

gpc_score.impact_score = gpc_score.impact_score.astype(int)
gpc_score['event_activity_score'] = gpc_score['read_2'] * gpc_score['impact_score']
gpc_score = gpc_score[['eid','event_activity_score']].groupby('eid').sum().reset_index()
gpc_score = pd.merge(gpc_score, num_eid, on = 'eid', how = 'inner')
gpc_score['activity_score'] = gpc_score.event_activity_score / gpc_score.eid_count
gpc_score = gpc_score[['eid','activity_score']]

# get score for full number of individuals
gpc_full = pd.read_csv('1_gpc_intersected.csv', sep = ',', dtype = str)
gpc_full = gpc_full[['eid','HEALTHY_CVD_T2D1_T2D2']]
gpc_full = gpc_full.drop_duplicates(keep = 'first')
gpc_score = gpc_score.astype(str)
gpc_full = pd.merge(gpc_full, gpc_score, on = 'eid', how = 'outer')
gpc_full = gpc_full.replace(np.nan, 0)

gpc_full.to_csv(path_or_buf = "3_activity_impact_score.csv" ,index = False, encoding = 'utf_8', line_terminator="\r\n")

print("stage 3 finished")





