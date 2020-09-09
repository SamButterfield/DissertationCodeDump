# -*- coding: utf-8 -*-
"""
@purpose Extracting Chapters at desired byte depth
@author: Sam Butterfield (UG)
"""

import pandas as pd
import numpy as np
from datetime import timedelta
import datetime

# ============================== Import Datasets ==============================
gpc_df = pd.read_csv('1_gpc_intersected.csv', sep = ',')
print("data imported")


mapped = gpc_df.copy()
mapped = mapped[['eid','event_dt','read_2','value1','value2','value3','HEALTHY_CVD_T2D1_T2D2']]
mapped['read_2'] = mapped['read_2'].replace('0', 'Y')
mapped['chapter'] = mapped['read_2'].astype(str).str[0]

# ============================== Filter Unwanted Chapters ==============================
exclusion = ['Z','Y','X','U', 'T', 'R','Q','P','L','_','.','9','8','5','4','2','1','0']
mapped = mapped[~mapped.chapter.isin(exclusion)]

# ============================== Filter 1-byte chapters ==============================
inclusion_1b = ['M']
mapped_1b = mapped.copy()
mapped_1b = mapped_1b[mapped_1b.chapter.isin(inclusion_1b)]
mapped_1b['read_extract'] = mapped_1b['read_2'].astype(str).str[0]
mapped_1b['read_extract'] = mapped_1b['read_extract'].astype(str) + "...."


# ============================== Filter 2-byte chapters ==============================
inclusion_2b = ['S','N','K','J','H','G','F','E','D','B','A']
mapped_2b = mapped.copy()
mapped_2b = mapped_2b[mapped_2b.chapter.isin(inclusion_2b)]
mapped_2b['read_extract'] = mapped_2b['read_2'].astype(str).str[0:2]
mapped_2b['read_extract'] = mapped_2b['read_extract'].astype(str) + "..."


# ============================== Filter 3-byte chapters ==============================
inclusion_3b = ['C']
mapped_3b = mapped.copy()
mapped_3b = mapped_3b[mapped_3b.chapter.isin(inclusion_3b)]
mapped_3b['read_extract'] = mapped_3b['read_2'].astype(str).str[0:3]
mapped_3b['read_extract'] = mapped_3b['read_extract'].astype(str) + ".."


# ============================== Filter 2-bytes Chapter 7 ==============================
inclusion_c7 = ['7']
inclusion_2b_c7 = ['70','71','72','73','74','75','76','77','78','79','7A','7B','7C','7D','7E','7F','7G','7H','7J','7K','7L','7P']
mapped_2b_c7 = mapped.copy()
mapped_2b_c7 = mapped_2b_c7[mapped_2b_c7.chapter.isin(inclusion_c7)]
mapped_2b_c7['read_extract'] = mapped_2b_c7['read_2'].astype(str).str[0:2]
mapped_2b_c7 = mapped_2b_c7[mapped_2b_c7.read_extract.isin(inclusion_2b_c7)]
mapped_2b_c7['read_extract'] = mapped_2b_c7['read_extract'].astype(str) + "..."


# ============================== Filter 2-bytes Chapter 6 ==============================
inclusion_c6 = ['6']
inclusion_2b_c6 = ['6C','6F']
mapped_2b_c6 = mapped.copy()
mapped_2b_c6 = mapped_2b_c6[mapped_2b_c6.chapter.isin(inclusion_c6)]
mapped_2b_c6['read_extract'] = mapped_2b_c6['read_2'].astype(str).str[0:2]
mapped_2b_c6 = mapped_2b_c6[mapped_2b_c6.read_extract.isin(inclusion_2b_c6)]
mapped_2b_c6['read_extract'] = mapped_2b_c6['read_extract'].astype(str) + "..."


# ============================== Filter 3-bytes Chapter 6 ==============================
inclusion_3b_c6 = ['661','662','663','669','66A','66C','66e','66f','66H','66n','66o','66Q','66X']
mapped_3b_c6 = mapped.copy()
mapped_3b_c6 = mapped_3b_c6[mapped_3b_c6.chapter.isin(inclusion_c6)]
mapped_3b_c6['read_extract'] = mapped_3b_c6['read_2'].astype(str).str[0:3]
mapped_3b_c6 = mapped_3b_c6[mapped_3b_c6.read_extract.isin(inclusion_3b_c6)]
mapped_3b_c6['read_extract'] = mapped_3b_c6['read_extract'].astype(str) + ".."


# ============================== Filter 3-bytes Chapter 3 ==============================
inclusion_c3 = ['3']
inclusion_2b_c3 = ['39']
mapped_2b_c3 = mapped.copy()
mapped_2b_c3 = mapped_2b_c3[mapped_2b_c3.chapter.isin(inclusion_c3)]
mapped_2b_c3['read_extract'] = mapped_2b_c3['read_2'].astype(str).str[0:2]
mapped_2b_c3 = mapped_2b_c3[mapped_2b_c3.read_extract.isin(inclusion_2b_c3)]
mapped_2b_c3['read_extract'] = mapped_2b_c3['read_extract'].astype(str) + "..."


# ============================== Join Data Back Together ==============================
gpc_concat_pop = pd.concat([mapped_1b,mapped_2b,mapped_3b,mapped_2b_c7,mapped_2b_c6,mapped_3b_c6,mapped_2b_c3])
gpc_concat_pop = gpc_concat_pop.astype(str)

# write to csv WHOLE POPULATION
#gpc_concat_pop.to_csv(path_or_buf = "2_gpc_code_extraction.csv" ,index = False, encoding = 'utf_8', line_terminator="\r\n")

# ======================================================================================================================================================
# ============================== Import activity df ==============================
activity_df = pd.read_csv('activityTimestamps.csv', sep = ',', dtype = str)
activity_df = activity_df.drop_duplicates(keep = 'first')
# ============================== Import Description Mapping ==============================
desc_map = pd.read_csv('read_2_lkp_distinct.csv', sep = ',', dtype = str)
desc_map = dict(desc_map[['read_code','term_description']].values)

# ============================== Take Events Surrounding Activity Timestamps ==============================
# Join the activity date to gp_clinical
gpc_trunc_activity = pd.merge(gpc_concat_pop, activity_df, on = 'eid',how = 'inner')
# Change columns to time and date types
gpc_trunc_activity['startDate'] = pd.to_datetime(gpc_trunc_activity['startDate'], yearfirst = True)
gpc_trunc_activity['endDate'] = pd.to_datetime(gpc_trunc_activity['endDate'], yearfirst = True)
gpc_trunc_activity['event_dt'] = pd.to_datetime(gpc_trunc_activity['event_dt'], yearfirst = True)
# Selecy only rows where event_dt is a year either side of startDate
gpc_trunc_activity = gpc_trunc_activity.loc[(gpc_trunc_activity['event_dt'] > (gpc_trunc_activity['startDate'] - timedelta(days = 182) )) & (gpc_trunc_activity['event_dt'] < (gpc_trunc_activity['endDate'] + timedelta(days = 30) )) ]


# ============================== Export Codes_impact_score sheet to edit ==============================
code_scoring = gpc_trunc_activity.read_extract.copy()
code_scoring.drop_duplicates(keep = 'first', inplace = True)
code_scoring = code_scoring.to_frame()
code_scoring['code_desc'] = code_scoring.read_extract.map(desc_map)
code_scoring.to_csv(path_or_buf = "code_impact_scoring.csv" ,index = False, encoding = 'utf_8', line_terminator="\r\n")

gpc_trunc_activity.to_csv(path_or_buf = "2_gpc_code_extraction_surrounding_timestamp.csv" ,index = False, encoding = 'utf_8', line_terminator="\r\n")




