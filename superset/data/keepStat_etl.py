import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy.dialects.mysql import INTEGER, VARCHAR, FLOAT, TINYINT, DOUBLE
from functools import reduce
import countries   # ../superset/data
import json

import logging

################
## PARAMETERS ##
################

logging.basicConfig(
        filename ='keep_statistics_etl.log',
        level = logging.INFO,
        format='%(asctime)s %(funcName)s : %(levelname)s : %(message)s'
        )

# DB

SQL_SOURCE_DB_USER = 'root'
SQL_SOURCE_DB_PSSW = 'keepKalm'
SQL_SOURCE_HOST = 'vpckeep2.arakne'
SQL_SOURCE_PATH = ("mysql://{0}:{1}@{2}/keep_drupal_burp"
        .format(SQL_SOURCE_DB_USER, SQL_SOURCE_DB_PSSW, SQL_SOURCE_HOST)
        )

SQL_DESTINATION_DB_USER = 'root'
SQL_DESTINATION_DB_PSSW = 'keepKalm'
SQL_DESTINATION_HOST = 'localhost'
SQL_DESTINATION_PATH = ("mysql://{0}:{1}@{2}/keep_drupal_burp"
        .format(SQL_DESTINATION_DB_USER, SQL_DESTINATION_DB_PSSW, SQL_DESTINATION_HOST)
        )

tables = [
        'keep_programme',
        'kamut_project', 'kamut_sotoip',
        'keep_programme_type',
        'kamut_partner',
        'taxonomy_term_data',
        'field_data_field_strand',
        'field_data_field_source',
        'field_data_field_keywords',
        'keep_nuts',
        'field_data_field_total_budget',
        'field_data_field_total_budget_2',
        'field_data_field_total_budget_ta',
        'field_data_field_them_prior',
        'field_data_field_them_obj',
        'field_data_field_strat_obj',
        'field_data_field_spec_obj',
        'field_data_field_spec_objs',
        'field_data_field_prog_prior',
        'field_data_field_lead_partner',
        'field_data_field_inv_prior',
        ]

#############
# FUNCTIONS #
#############

def extractThematic(field_data_keywords, taxonomy_term_data):
    '''
    Ref: keep_drupal_burp db
    Function that extracts thematic given tables field_data_keywords
    and taxonomy_term_data
    '''
    df_thematics = field_data_field_keywords.copy()
    idx_project_entity = df_thematics[df_thematics.entity_type == 'kamut_project'].index
    df_thematics = df_thematics.loc[idx_project_entity]
    df_thematics.rename(
            {'entity_id':'project_id', 'field_keywords_tid':'tid'},
            axis = 1,
            inplace = True
            )
    df_thematics = df_thematics[['project_id', 'tid']]
    return df_thematics.set_index('tid').join(
            taxonomy_term_data.set_index('tid'),
            how='left'
            ).reset_index(drop = True)

def extractProgramType(field_data_field_strand,
        field_data_field_source, taxonomy_term_data):
    '''
    Ref: keep_drupal_burp db
    Function that extracts Programme Type given tables field_data_field_strand,
    field_data_field_source and taxonomy_term_data.
    '''
    # strand
    idx_programme = field_data_field_strand[
            field_data_field_strand.entity_type == 'keep_programme'
            ].index
    strand = field_data_field_strand.loc[idx_programme]
    strand = strand[['field_strand_tid', 'entity_id']]
    strand.rename(
                {'field_strand_tid':'taxonomy_term', 'entity_id':'programme_id'},
                axis = 1,
                inplace = True
            )
    idx_strand_term = field_data_field_strand[
            field_data_field_strand.entity_type =='taxonomy_term'
            ].index
    strand_term = field_data_field_strand.loc[idx_strand_term]
    strand_term = strand_term[['field_strand_tid', 'entity_id']]
    strand_term.rename(
                {'entity_id':'tid', 'field_strand_tid':'taxonomy_term'},
                axis = 1,
                inplace = True
            )
    idx_programme = field_data_field_source[
            field_data_field_source.entity_type == 'keep_programme'
            ].index

    # source
    source = field_data_field_source.loc[idx_programme]
    source = source[['field_source_tid', 'entity_id']]
    source.rename(
                {'field_source_tid':'taxonomy_term', 'entity_id':'programme_id'},
                axis = 1,
                inplace = True
            )
    idx_source_term = field_data_field_source[
                field_data_field_source.entity_type =='taxonomy_term'
            ].index
    source_term = field_data_field_source.loc[idx_source_term]
    source_term = source_term[['field_source_tid', 'entity_id']]
    source_term.rename(
                {'entity_id':'tid', 'field_source_tid':'taxonomy_term'},
                axis = 1,
                inplace = True
            )

    source_tid = source.merge(source_term, on = 'taxonomy_term')[
        ['programme_id', 'tid']
        ]

    strand_tid = strand.merge(strand_term, on = 'taxonomy_term')[
        ['programme_id', 'tid']]

    program_type_tid = source_tid.merge(strand_tid,
                on = ['programme_id', 'tid'])

    program_type = program_type_tid.merge(
            taxonomy_term_data,
            on = 'tid')[['programme_id', 'name']]
    program_type.rename(
            {'name':'program_type'},
            axis  = 1,
            inplace = True)
    return program_type

def commitTable(df_table, table_name, engine,
        dtype_flag = False, dtype_dic = None ):
    table_name = 'superset_'+table_name
    if not dtype_flag:
        df_table.to_sql(
            con = engine.connect(),
            name = table_name,
            if_exists = 'replace',
            index = False,
            )
    else:
        df_table.to_sql(
            con = engine.connect(),
            name = table_name,
            if_exists = 'replace',
            index = False,
            dtype = dtype_dic
        )

def merger(tableList):
    return reduce(
                lambda x,y : pd.merge(x,y, how = 'outer', on = 'project_id'),
                tableList)

def getTables(table_list, engine):
    cnx = engine.connect()
    out = {}
    for table_name in table_list:
        out[table_name] = pd.read_sql_table(table_name, cnx)
    cnx.close()
    return out

############
##  MAIN  ##
############

logging.info('Start elaborating database')
logging.info('Create SQL engines')

mysql_engine_destination = create_engine(SQL_DESTINATION_PATH)
mysql_engine_source = create_engine(SQL_SOURCE_PATH)

dfs = getTables(tables, mysql_engine_source)

logging.info('Start elaboration')

### SELECT TABLES FEATURES
## keep_programme
keep_programme = dfs['keep_programme'][
                ['id', 'type', 'title', 'use_for_statistics', 'is_visible']
        ].copy()
keep_programme = keep_programme[(keep_programme.use_for_statistics == 1)]

## keep_project
kamut_project = dfs['kamut_project'][
                ['pid', 'programme_id', 'budget', 'project_start', 'project_end']
        ].copy()

# Replace 0 with nan to avoid misleading time conversion
kamut_project.project_start = kamut_project.project_start.replace(0,np.nan)
kamut_project.project_end = kamut_project.project_end.replace(0,np.nan)

## keep_programme_type
keep_programme_type = dfs['keep_programme_type'][['type', 'period']].copy()

## kamut_partner
kamut_partner = dfs['kamut_partner'][
        ['pid',
        'project_id',
        'partnership_type',
        'country',
        'region_0',
        'region_1',
        'region_2',
        'region_3',
        'is_ext_nuts',
        'geocoding_x',
        'geocoding_y']].copy()
 # geocoding_y is latitude, geocoding_x longitude

## kamut_sotoip
kamut_sotoip = dfs['kamut_sotoip'][
        ['programme_id',
        'thematic_objective',
        'investment_priority']
    ].copy()


# zfill string to sort correctly
kamut_sotoip['thematic_objective'] = (
            '('+
            kamut_sotoip.thematic_objective.str.extract(r'([0-9]+)',
            expand = False).str.zfill(2)+')' +
            kamut_sotoip.thematic_objective.str.replace('(\([0-9]+\))', '')
        )

kamut_sotoip['investment_priority'] = (
            '(' +
            kamut_sotoip.investment_priority.str.extract(r'([0-9]+)',
            expand = False).str.zfill(2) +
            kamut_sotoip.investment_priority.str.replace('(\([0-9]+)', '')
        )

# Getting max length for thematic objective and investment_priority fields
varchar_max = np.max(kamut_sotoip.iloc[:,[1,2]].fillna('o').applymap(
    lambda x : np.array(len(x))).values
    )

kamut_sotoip.drop_duplicates(inplace = True)
kamut_sotoip = kamut_sotoip[~kamut_sotoip.isnull()[
        ['thematic_objective', 'investment_priority']
    ].all(axis = 1)]

taxonomy_term_data = dfs['taxonomy_term_data'][['tid', 'name']].copy()

## field_data_field_strand
field_data_field_strand = dfs['field_data_field_strand'][
            ['entity_type', 'bundle','entity_id', 'field_strand_tid']
        ].copy()

## field_data_field_source
field_data_field_source = dfs['field_data_field_source'][
            ['entity_type', 'bundle', 'entity_id', 'field_source_tid']
        ].copy()

## field_data_field_keywords
field_data_field_keywords = dfs['field_data_field_keywords'][
            ['entity_type', 'bundle', 'entity_id', 'field_keywords_tid']
        ].copy()

## keep_nuts
keep_nuts = dfs['keep_nuts'][
            ['entity_id', 'nuts_id', 'country_code', 'parent', 'description']
        ].copy()

### TABLES ELABORATION ###

## Projects metric
projectId_programmeId = kamut_project[['pid', 'programme_id']].copy()
projectId_programmeId.rename({'pid':'project_id'}, axis = 1, inplace = True)

## ProjectID-Thematics
projectId_thematic = extractThematic(field_data_field_keywords, taxonomy_term_data)
projectId_thematic.rename({'name':'thematic'}, axis = 1, inplace = True)
projectId_thematic.dropna(inplace = True)

## ProjectID - Country/NUTS/Partners

projectId_countryNutsPartners = kamut_partner[
            [
                'project_id',
                'pid',
                'country',
                'region_0',
                'region_1',
                'region_2',
                'region_3',
                'geocoding_x',
                'geocoding_y',
                'partnership_type',
                'is_ext_nuts'
            ]
        ].copy()

projectId_countryNutsPartners.rename(
        {
            'pid':'partner_id',
            'country':'country_name',
            'region_0':'nuts_0',
            'region_1':'nuts_1',
            'region_2':'nuts_2',
            'region_3':'nuts_3',
            'geocoding_x':'partner_lgt',
            'geocoding_y':'partner_lat',
            'partnership_type':'is_leader'},
        axis = 1,
        inplace = True)


projectId_countryNutsPartners.drop_duplicates(inplace = True)

projectId_countryNutsPartners['cca2'] = projectId_countryNutsPartners.nuts_0.replace(
            {'UK':'GB', 'EL':'GR'}
        )
projectId_countryNutsPartners['lat'] = projectId_countryNutsPartners.cca2.dropna().apply(
        lambda x : countries.get('cca2', x)['lat']
        )
projectId_countryNutsPartners['lng'] = projectId_countryNutsPartners.cca2.dropna().apply(
        lambda x : countries.get('cca2', x)['lng']
        )


projectId_countryNutsPartners = projectId_countryNutsPartners[
        ~projectId_countryNutsPartners.project_id.isnull()
        ]

## ProjectID - Programming Period
programmeID_programmingPeriod = keep_programme[['id', 'type']].merge(
        keep_programme_type,
        on = 'type'
        )[['id', 'period']]
programmeID_programmingPeriod.rename(
        {'id':'programme_id'},
        axis = 1,
        inplace = True)

projectId_period = programmeID_programmingPeriod.merge(
        projectId_programmeId,
        on = 'programme_id'
        )[['project_id', 'period']]

## ProjectID - Programme Names
projectId_programmeName = keep_programme.rename({'id':'programme_id'}, axis = 1).merge(
            kamut_project[['pid', 'programme_id']],
            on = 'programme_id'
            )[['pid', 'title']].rename(
                        {'pid':'project_id', 'title':'programme_name'},
                        axis = 1
                        )

## ProjectID - Program Type
programmeId_programType = extractProgramType(
        field_data_field_strand,
        field_data_field_source,
        taxonomy_term_data
        )

projectId_programType = programmeId_programType.merge(
        projectId_programmeId,
        on = 'programme_id')[['project_id', 'program_type']]

## ProjectID - Thematic Objectives and Investement Priorities
programmeId_thObj_invPri = kamut_sotoip.drop_duplicates()

mask_all_nan = programmeId_thObj_invPri[
            ['thematic_objective', 'investment_priority']
            ].applymap(lambda x : x == None).all(axis = 1)

programmeId_thObj_invPri = programmeId_thObj_invPri[~mask_all_nan]

projectId_thObj_invPrio = programmeId_thObj_invPri.merge(
        projectId_programmeId,
        on = 'programme_id'
        )[['project_id','thematic_objective', 'investment_priority']]

# zfill string to sort correctly
projectId_thObj_invPrio['thematic_objective'] = (
        '('+
        projectId_thObj_invPrio.thematic_objective.str.extract(r'([0-9]+)',
        expand = False
        ).str.zfill(2)+')' +
         projectId_thObj_invPrio.thematic_objective.str.replace('(\([0-9]+\))', ''))

projectId_thObj_invPrio['investment_priority'] = (
        '(' +
        projectId_thObj_invPrio.investment_priority.str.extract(r'([0-9]+)',
        expand = False
        ).str.zfill(2)+projectId_thObj_invPrio.investment_priority.str.replace('(\([0-9]+)', ''))

# Getting max length for thematic objective and investment_priority fields
varchar_max = np.max(
        projectId_thObj_invPrio.iloc[:,[1,2]].fillna('o').applymap(
            lambda x : np.array(len(x))
            ).values)

## ProjectID - Budget

projectId_budget = kamut_project[['pid', 'budget']].copy()
projectId_budget.rename({'pid':'project_id'}, axis = 1, inplace = True)

## ProjectID - StartEnd
projectId_startEnd = kamut_project[['pid', 'project_start', 'project_end']].copy()
projectId_startEnd.dropna(inplace = True)
projectId_startEnd.rename({'pid':'project_id'}, axis = 1, inplace = True)

### FURTHER ELABORATION AND DB COMMIT

## MASTER ##
tables_to_merge = [
        projectId_thematic,
        projectId_countryNutsPartners,
        projectId_period,
        projectId_programmeId,
        projectId_programmeName,
        projectId_programType,
        projectId_thObj_invPrio,
        projectId_startEnd,
        projectId_budget
        ]

master = merger(tables_to_merge)
master.drop_duplicates(inplace = True)

# Set duplicated budget to zero
master['budget'][master[['project_id', 'thematic', 'period', 'budget']].duplicated()] = 0

name_map_dic = keep_nuts[['nuts_id', 'description']].set_index("nuts_id").to_dict()['description']

master[['nuts_0', 'nuts_1', 'nuts_2']] = master[['nuts_0', 'nuts_1', 'nuts_2']].fillna('nan')
master[['nuts_0', 'nuts_1', 'nuts_2']] = master[['nuts_0', 'nuts_1', 'nuts_2']].applymap(
        lambda x : '['+x+'] - '+name_map_dic[x].lower().capitalize() if x != 'nan' else np.nan
        )
logging.info('Commit to superset_master')
# Commit Table on DESTINATION_DB

coord_dict = pd.read_csv('./nuts_coord.csv', index_col = 0).set_index('NUTS_code').to_dict()

master['nuts0_lat'] = master['nuts_0'].str.slice(1,3).apply(lambda x : coord_dict['lat'].get(x, np.nan))
master['nuts0_lng'] = master['nuts_0'].str.slice(1,3).apply(lambda x : coord_dict['lng'].get(x, np.nan))

master['nuts1_lat'] = master['nuts_1'].str.slice(1,4).apply(lambda x : coord_dict['lat'].get(x, np.nan))
master['nuts1_lng'] = master['nuts_1'].str.slice(1,4).apply(lambda x : coord_dict['lng'].get(x, np.nan))

master['nuts2_lat'] = master['nuts_2'].str.slice(1,5).apply(lambda x : coord_dict['lat'].get(x, np.nan))
master['nuts2_lng'] = master['nuts_2'].str.slice(1,5).apply(lambda x : coord_dict['lng'].get(x, np.nan))

master['nuts3_lat'] = master['nuts_3'].apply(lambda x : coord_dict['lat'].get(x, np.nan))
master['nuts3_lng'] = master['nuts_3'].apply(lambda x : coord_dict['lng'].get(x, np.nan))

# TODO esplicitare lo schema di export della tabella

commitTable(
         master,
         'master',
         mysql_engine_destination,
         dtype_flag = True,
         dtype_dic = {
             'project_id':INTEGER,
             'thematic':VARCHAR(255),
             'partner_id':INTEGER,
             'country_name':VARCHAR(255),
             'nuts_0':VARCHAR(255),
             'nuts_1':VARCHAR(255),
             'nuts_2':VARCHAR(255),
             'nuts_3':VARCHAR(255),
             'partner_lgt':DOUBLE,
             'partner_lat':DOUBLE,
             'is_leader':TINYINT(4),
             'is_ext_nuts':TINYINT(4),
             'cca2':VARCHAR(255),
             'lat':DOUBLE,
             'lng':DOUBLE,
             'period':VARCHAR(255),
             'programme_id':INTEGER,
             'programme_name':VARCHAR(255),
             'program_type':VARCHAR(255),
             'thematic_objective':VARCHAR(varchar_max),
             'investment_priority':VARCHAR(varchar_max),
             'project_start':DOUBLE,
             'project_end':DOUBLE,
             'budget':DOUBLE, 
             'nuts0_lat': DOUBLE,
             'nuts0_lng': DOUBLE,
             'nuts1_lat': DOUBLE,
             'nuts1_lng': DOUBLE,
             'nuts2_lat': DOUBLE,
             'nuts2_lng': DOUBLE,
             'nuts3_lat': DOUBLE,
             'nuts3_lng': DOUBLE,
             }
         )

INDICES_QUERY = "ALTER TABLE superset_master ADD INDEX(project_id), ADD INDEX(thematic),  ADD INDEX(partner_id), ADD INDEX(country_name), ADD INDEX(nuts_0), ADD INDEX(nuts_1), ADD INDEX(nuts_2), ADD INDEX(nuts_3), ADD INDEX(period), ADD INDEX(programme_id), ADD INDEX(programme_name), ADD INDEX(program_type), ADD INDEX(thematic_objective), ADD INDEX(investment_priority);"


with mysql_engine_destination.connect() as con:
        con.execute(INDICES_QUERY)

logging.info('Done!')



logging.shutdown()