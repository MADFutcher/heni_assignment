import pandas as pd
import utils
import uuid
import db_helper
from config import config


# Get db Config from Configration.ini and connnect.
db_info = config(section='dbConfig')
conn = db_helper.create_connection(db_info['dblocation']+db_info['dbname'])

db_helper.create_table(conn, db_info['tag_table'], db_info['tag_schema'])
db_helper.create_table(conn, db_info['webpage_table'], db_info['webpage_schema'])

# Create lists for webpage and tag
webpage_dfs = []
tag_dfs = []

# Read CSV
crawler_lookup_df = pd.read_csv('./commoncrawl_lookup.csv')
crawler_lookup_df[['source_length', 'source_offset']] = crawler_lookup_df[['source_length', 'source_offset']].fillna(0)


for idx in crawler_lookup_df.index:
    row = crawler_lookup_df.iloc[idx]

    source_url = row['source_url']
    url = row['url']
    offset = int(row['source_offset'])
    length = int(row['source_length'])
    
    source_exists = db_helper.check_url(url, conn, source=False)

    
    if not source_exists:
        print(f'scrapping tags for URL: {url}')
        url_id = uuid.uuid4().hex
        if source_url != '' and offset != 0 and length != 0:
            html = utils.load_single_warc_record(
                source=source_url, offset=offset, length=length
            )

            webpage_data = {'url_id':[url_id], 'url':[url], 'source_url':[source_url], 'source_offset':[offset], 'source_length':[length]}
            webpage_df = pd.DataFrame(data=webpage_data)   
            all_nodes = utils.get_nodes(html)

            for node in all_nodes:
                node_tags = utils.get_node_tags(node)
                parent_tags = utils.get_parent_tags(node)
                left_sib = utils.get_left_sibling_tags(node)
                right_sib = utils.get_right_sibling_tags(node)

                for nodeTag in node_tags:
                    for parent in parent_tags:
                        for leftSib in left_sib:
                            for rightSib in right_sib:
                                tag_data = d = {'url_id': [url_id],
                                            'tag':[nodeTag],'parent_tag':[parent],
                                            'left_sibling':[leftSib], 'right_sibling':[rightSib]
                                            }
                                
                                tags_df = pd.DataFrame(data=tag_data)            
                                tag_dfs.append(tags_df)
    else:

        
        print(f'{url} was already found in the DB.')

if len(webpage_df) >=1:
    full_webpages_df = pd.concat(webpage_dfs).reset_index(drop=True)
    db_helper.insert_data(full_webpages_df,db_info['webpage_table'], conn)

if len(tag_dfs) >=1:
    full_tag_df = pd.concat(tag_dfs).reset_index(drop=True).applymap(str)
    db_helper.insert_data(full_tag_df, db_info['tag_table'], conn)

conn.close()

print('completed!')
