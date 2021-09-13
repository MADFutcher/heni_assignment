import os

import pandas as pd

from legalutils.database import get_connection
from utils import load_single_warc_record


FILE_LOC = os.path.expanduser(os.path.join('~', 'jha', 'parquet_test', 'commoncrawl_lookup.csv'))


cnxn = get_connection(user='research')
query = '''select url, source_url, source_offset, source_length
          from news_manual_edit
          where collected_by like '%CommonCrawl%'
          order by random()
          '''
df_nme = pd.read_sql(query, cnxn)
cnxn.close()

# Load a single URL
html_lkup = df_nme.iloc[0]

# Note: using int is a trick
html = load_single_warc_record(
    source=html_lkup['source_url'], offset=int(html_lkup['source_offset']), length=int(html_lkup['source_length'])
)

# Challenges:

# 1) Not all articles have available lookup in CommonCrawl
# 2) The offset / lengths are stored as floats (must be cast to int)
# 3) Duplicated rows (for URLs ending in 'x'):

df_dup = df_nme[df_nme['url'].apply(lambda url: url.endswith('x'))]
df_nme = pd.concat([df_nme, df_dup], axis=0)
df_nme.to_csv(FILE_LOC, index=False)
