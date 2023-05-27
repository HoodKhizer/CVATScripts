import psycopg2
import os
# Filenames to query for are neatly stored in separate file
# The idea is to just copy the generated list as is 
# and reduce the manual effort of formatting the list
# in a suitable form for input
# Get list of filenames -> paste in images_names.py
# ???
# PROFIT!
from images_names import images_names

out_file = 'outlinks.txt' # File to which urls will be written
cvat_url = '172.16.128.67' # IP of cvat server
cvat_port = '8080' # port of cvat server

conn = psycopg2.connect(
    host='172.19.0.8', # IP of cvat_db container #
    port="5432", # Port at which postgres runs #
    database="cvat", # cvat_db creds
    user="root",
)
cur = conn.cursor()

out_links = []

for name in images_names:
    query = f"select concat('http://{cvat_url}:{cvat_port}/tasks/', engine_task.id, '/jobs/', engine_segment.id, '?frame=', engine_image.frame), engine_image.frame, engine_task.id, engine_segment.id from engine_image inner join engine_task on engine_image.data_id = engine_task.data_id inner join engine_segment on engine_task.id =engine_segment.task_id where path='{name}' and engine_segment.start_frame <= engine_image.frame and engine_image.frame <= engine_segment.stop_frame;"
    cur.execute(query)

    for row in cur.fetchall():
        out_links.append(row[0])

# write to file
with open(out_file, "w", encoding="utf-8") as f:
    f.write("\n".join(out_links))
cur.close()
conn.close()
