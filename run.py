import google.cloud.bigquery
import psycopg2
import os
import mysql.connector
import time

begin = time.time()

cnx = google.cloud.bigquery.Client(project='jfp-data-warehouse')
qry = cnx.query(
    '''
    SELECT MAX(timestamp)
    FROM `jfp-data-warehouse.redshift.me2_event`
    '''
)
qry = qry.result()
(last,), = qry

cnx2 = psycopg2.connect(os.environ['POSTGRES_STR'])
qry2 = cnx2.cursor()
qry2.execute(
    '''
    SELECT *
    FROM jfp_stage.me2_event
    WHERE timestamp > {}
    '''.format(last)
)

while qry2.rownumber < qry2.rowcount:
    cnx.insert_rows(cnx.get_table('jfp-data-warehouse.redshift.me2_event'), qry2.fetchmany(9999))

qry = cnx.query(
    '''
    SELECT MAX(tracker_log_time)
    FROM `jfp-data-warehouse.redshift.me2_share_event`
    '''
)
qry = qry.result()
(last,), = qry

qry2.execute(
    '''
    SELECT *
    FROM me2_share.event
    WHERE tracker_log_time > '{}'
    '''.format(last)
)

while qry2.rownumber < qry2.rowcount:
    cnx.insert_rows(cnx.get_table('jfp-data-warehouse.redshift.me2_share_event'), qry2.fetchmany(9999))

qry2.execute(
    '''
    SELECT tb.*
    FROM jfp_stage.me2_custom tb
    LEFT JOIN me2_share.event tb2 ON tb2.tracker_log_id = tb.tracker_log_id
    WHERE tb2.tracker_log_time > '{}'
    '''.format(last)
)

while qry2.rownumber < qry2.rowcount:
    cnx.insert_rows(cnx.get_table('jfp-data-warehouse.redshift.me2_custom'), qry2.fetchmany(9999))

cnx_arg = os.environ['YOUTUBE_STR']
cnx_arg = cnx_arg.split(' ')
cnx_arg = dict((arg.split('=') for arg in cnx_arg))
cnx2 = mysql.connector.connect(**cnx_arg)
qry2 = cnx2.cursor()

qry = cnx.query(
    '''
    SELECT DISTINCT(event_date)
    FROM `jfp-data-warehouse.mysql.bi_view_youtube_plays_date_country_jfm`
    ORDER BY event_date DESC
    LIMIT 2
    '''
)
qry = qry.result()
_, (last,) = qry

cnx.query(
    '''
    DELETE FROM `jfp-data-warehouse.mysql.bi_view_youtube_plays_date_country_jfm`
    WHERE event_date > '{}'
    '''.format(last)
)

qry2.execute(
    '''
    SELECT *
    FROM jfp_analytics_prod.bi_view_youtube_plays_date_country_jfm
    WHERE event_date > '{}'
    '''.format(last)
)

while True:
    plays = qry2.fetchmany(9999)
    if len(plays) == 0:
        break

    cnx.insert_rows(cnx.get_table('jfp-data-warehouse.mysql.bi_view_youtube_plays_date_country_jfm'), plays)

qry = cnx.query(
    '''
    SELECT DISTINCT(event_date)
    FROM `jfp-data-warehouse.mysql.bi_view_youtube_plays_date_country_mcn`
    ORDER BY event_date DESC
    LIMIT 2
    '''
)
qry = qry.result()
_, (last,) = qry

cnx.query(
    '''
    DELETE FROM `jfp-data-warehouse.mysql.bi_view_youtube_plays_date_country_mcn`
    WHERE event_date > '{}'
    '''.format(last)
)

qry2.execute(
    '''
    SELECT *
    FROM jfp_analytics_prod.bi_view_youtube_plays_date_country_mcn
    WHERE event_date > '{}'
    '''.format(last)
)

while True:
    plays = qry2.fetchmany(9999)
    if len(plays) == 0:
        break

    cnx.insert_rows(cnx.get_table('jfp-data-warehouse.mysql.bi_view_youtube_plays_date_country_mcn'), plays)

tables = (
    ('bi_test_users', 'bi_test_users'),
    ('bi_view_media_component', 'bi_view_media_component_v2'),
    ('bi_view_media_component_language', 'bi_view_media_component_language_v2'),
    ('bi_view_media_component_language_version_log', 'bi_view_media_component_language_version_log_v2'),
    ('bi_view_media_language', 'bi_view_media_language_v2'),
    ('bi_view_youtube_channel', 'bi_view_youtube_channel'),
    ('bi_view_youtube_video', 'bi_view_youtube_video')
)
for i in range(len(tables)):
    cnx.query(
        '''
        TRUNCATE TABLE `jfp-data-warehouse.mysql.{}`
        '''.format(tables[i][0])
    )

    qry2.execute(
        '''
        SELECT *
        FROM jfp_analytics_prod.{}
        '''.format(tables[i][1])
    )

    while True:
        plays = qry2.fetchmany(9999)
        if len(plays) == 0:
            break
        
        cnx.insert_rows(cnx.get_table('jfp-data-warehouse.mysql.' + tables[i][0]), plays)

print(time.time() - begin)