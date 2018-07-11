'''
Spatial validation of ROAD and ROAD_INFRASTRUCTURE.

Usage:
  transport_spatial_validation.py [options]

Options:
  --estamap_version <version>  ESTAMap Version
  --log_file <file>       Log File name. [default: transport_spatial_validation.log]
  --log_path <folder>     Folder to store the log file. [default: c:\\temp]
'''
import os
import sys
import time
import logging
import shutil

from docopt import docopt
import arcpy
import lmdb
import networkx as nx

import matplotlib.pyplot as plt

import log
import dev as gis
import dbpy


def import_road_patch(estamap_version):

    if estamap_version == 'CORE':
        raise Exception('use another estamap version')

    logging.info('environment')
    em_core = gis.ESTAMAP('CORE')
    em = gis.ESTAMAP(estamap_version)

    if arcpy.Exists(os.path.join(em.sde, 'ROAD_PATCH')):
        arcpy.Delete_management(os.path.join(em.sde, 'ROAD_PATCH'))
    
    logging.info('importing ROAD_PATCH: {}'.format(os.path.join(em_core.sde, 'ROAD_PATCH')))
    arcpy.FeatureClassToFeatureClass_conversion(in_features=os.path.join(em_core.sde, 'ROAD_PATCH'),
                                                out_path=em.sde,
                                                out_name='ROAD_PATCH')
    road_patch_count = arcpy.GetCount_management(os.path.join(em.sde, 'ROAD_PATCH')).getOutput(0)
    logging.info('ROAD_PATCH count: {}'.format(road_patch_count))


def transport_spatial_validation(estamap_version, with_patch=False):

    logging.info('environment')
    em = gis.ESTAMAP(estamap_version)
    arcpy.env.XYTolerance = '0.01 Meters'
    fgdb_name = 'TransportSpatialValidation_{}.gdb'.format('withPatch' if with_patch else 'exclPatch')
    fgdb = os.path.join(em.path, 'Routing', fgdb_name)
    

    if not os.path.exists(os.path.join(em.path, 'Routing')):
        logging.info('creating Routing folder: {}'.format(os.path.join(em.path, 'Routing')))
        os.makedirs(os.path.join(em.path, 'Routing'))

    logging.info('creating fgdb: {}'.format(os.path.join(em.path, 'Routing', fgdb_name)))
    if arcpy.Exists(os.path.join(em.path, 'Routing', fgdb_name)):
        arcpy.Delete_management(os.path.join(em.path, 'Routing', fgdb_name))
    arcpy.CreateFileGDB_management(out_folder_path=os.path.join(em.path, 'Routing'),
                                   out_name=fgdb_name)

    logging.info('creating feature dataset')
    arcpy.CreateFeatureDataset_management(out_dataset_path=os.path.join(em.path, 'Routing', fgdb_name),
                                          out_name='FD',
                                          spatial_reference=arcpy.SpatialReference(3111))

    logging.info('importing ROAD')
    arcpy.FeatureClassToFeatureClass_conversion(in_features=os.path.join(em.sde, 'ROAD'),
                                                out_path=os.path.join(em.path, 'Routing', fgdb_name, 'FD'),
                                                out_name='ROAD')
    road_count = arcpy.GetCount_management(os.path.join(em.path, 'Routing', fgdb_name, 'FD', 'ROAD')).getOutput(0)
    logging.info('ROAD records: {}'.format(road_count))

    logging.info('importing ROAD_INFRASTRUCTURE')
    arcpy.FeatureClassToFeatureClass_conversion(in_features=os.path.join(em.sde, 'ROAD_INFRASTRUCTURE'),
                                                out_path=os.path.join(em.path, 'Routing', fgdb_name, 'FD'),
                                                out_name='ROAD_INFRASTRUCTURE')
    road_infrastructure_count = arcpy.GetCount_management(os.path.join(em.path, 'Routing', fgdb_name, 'FD', 'ROAD_INFRASTRUCTURE')).getOutput(0)
    logging.info('ROAD_INFRASTRUCTURE records: {}'.format(road_infrastructure_count))


    in_source_feature_classes = 'ROAD SIMPLE_EDGE NO;ROAD_INFRASTRUCTURE SIMPLE_JUNCTION NO'
    if with_patch:
        in_source_feature_classes = in_source_feature_classes + ';ROAD_PATCH SIMPLE_EDGE NO'
        
        logging.info('importing ROAD_PATCH')
        arcpy.FeatureClassToFeatureClass_conversion(in_features=os.path.join(em.sde, 'ROAD_PATCH'),
                                                    out_path=os.path.join(em.path, 'Routing', fgdb_name, 'FD'),
                                                    out_name='ROAD_PATCH',
                                                    where_clause='"MERGE_TRANSPORT" = 1')
        road_patch_count = arcpy.GetCount_management(os.path.join(em.path, 'Routing', fgdb_name, 'FD', 'ROAD_PATCH')).getOutput(0)
        logging.info('ROAD_PATCH records: {}'.format(road_patch_count))

##        logging.info('importing ROAD_INFRASTRUCTURE_PATCH')
##        arcpy.MakeFeatureLayer_management(in_features=os.path.join(em.sde, 'ROAD_INFRASTRUCTURE_PATCH'),
##                                          out_layer='in_memory\\road_infrastructure_patch_layer',
##                                          where_clause='"MERGE_TRANSPORT" = 1')
##        arcpy.FeatureClassToFeatureClass_conversion(in_features='in_memory\\road_infrastructure_patch_layer',
##                                                    out_path=os.path.join(em.path, 'Routing', fgdb_name, 'FD'),
##                                                    out_name='ROAD_INFRASTRUCTURE_PATCH')

    
    logging.info('creating geometric network: {}'.format(os.path.join(em.path, 'Routing', fgdb_name, 'FD', 'geonet')))
    arcpy.CreateGeometricNetwork_management(in_feature_dataset=os.path.join(em.path, 'Routing', fgdb_name, 'FD'),
                                            out_name='geonet',
                                            in_source_feature_classes=in_source_feature_classes, 
                                            preserve_enabled_values='PRESERVE_ENABLED')

##    arcpy.AddEdgeEdgeConnectivityRuleToGeometricNetwork_management(in_geometric_network=os.path.join(em.path, 'Routing', fgdb_name, 'FD', 'geonet'),
##                                                                   in_from_edge_feature_class="ROAD",
##                                                                   from_edge_subtype="ROAD",
##                                                                   in_to_edge_feature_class="ROAD",
##                                                                   to_edge_subtype="ROAD",
##                                                                   in_junction_subtypes="'ROAD_INFRASTRUCTURE : ROAD_INFRASTRUCTURE'",
##                                                                   default_junction_subtype="ROAD_INFRASTRUCTURE : ROAD_INFRASTRUCTURE")
##
##    arcpy.AddEdgeJunctionConnectivityRuleToGeometricNetwork_management(in_geometric_network=os.path.join(em.path, 'Routing', fgdb_name, 'FD', 'geonet'),
##                                                                       in_edge_feature_class="ROAD",
##                                                                       edge_subtype="ROAD",
##                                                                       in_junction_feature_class="ROAD_INFRASTRUCTURE",
##                                                                       junction_subtype="ROAD_INFRASTRUCTURE",
##                                                                       default_junction="DEFAULT",
##                                                                       edge_min="",
##                                                                       edge_max="",
##                                                                       junction_min="",
##                                                                       junction_max="")
##
##    # DC by: find dc function
##
##    logging.info('finding disconnected: ROAD')
##    arcpy.FindDisconnectedFeaturesInGeometricNetwork_management(in_layer=os.path.join(em.path, 'Routing', fgdb_name, 'FD', 'ROAD'),
##                                                                out_layer='in_memory\\ROAD_disconnected')
##    if arcpy.Exists('in_memory\\ROAD_disconnected'):
##        fdc_road_count = arcpy.GetCount_management('in_memory\\ROAD_disconnected').getOutput(0)
##        logging.info('found ROAD disconnected: {}'.format(fdc_road_count))
##
##        arcpy.FeatureClassToFeatureClass_conversion(in_features='in_memory\\ROAD_disconnected',
##                                                    out_path=os.path.join(em.path, 'Routing', fgdb_name),
##                                                    out_name='ROAD_disconnected')
##
##    logging.info('finding disconnected: ROAD_INFRASTRUCTURE')
##    arcpy.FindDisconnectedFeaturesInGeometricNetwork_management(in_layer=os.path.join(em.path, 'Routing', fgdb_name, 'FD', 'ROAD_INFRASTRUCTURE'),
##                                                                out_layer='in_memory\\ROAD_INFRASTRUCTURE_disconnected')
##    if arcpy.Exists('in_memory\\ROAD_INFRASTRUCTURE_disconnected'):
##        fdc_road_infrastructure_count = arcpy.GetCount_management('in_memory\\ROAD_INFRASTRUCTURE_disconnected').getOutput(0)
##        logging.info('found ROAD_INFRASTRUCTURE disconnected: {}'.format(fdc_road_infrastructure_count))
##
##        arcpy.FeatureClassToFeatureClass_conversion(in_features='in_memory\\ROAD_INFRASTRUCTURE_disconnected',
##                                                    out_path=os.path.join(em.path, 'Routing', fgdb_name),
##                                                    out_name='ROAD_INFRASTRUCTURE_disconnected')

    
    # DC by: tracing 
    logging.info('creating starting point')
    arcpy.CreateFeatureclass_management(out_path=os.path.join(em.path, 'Routing', fgdb_name),
                                        out_name='StartingPoint',
                                        geometry_type='POINT',
                                        spatial_reference=arcpy.SpatialReference(3111))
    with arcpy.da.InsertCursor(in_table=os.path.join(em.path, 'Routing', fgdb_name, 'StartingPoint'),
                               field_names=['SHAPE@']) as ic:
        pt = arcpy.Point(2497133.064, 2409284.931)
        ic.insertRow((pt,))


    logging.info('Tracing Geometric Network...')
    arcpy.TraceGeometricNetwork_management(in_geometric_network=os.path.join(em.path, 'Routing', fgdb_name, 'FD', 'geonet'),
                                           out_network_layer='in_memory\\geonet_trace_output',
                                           in_flags=os.path.join(em.path, 'Routing', fgdb_name, 'StartingPoint'),                                           
                                           in_trace_task_type='FIND_DISCONNECTED',
                                           in_trace_ends='NO_TRACE_ENDS',
                                           in_trace_indeterminate_flow='NO_TRACE_INDETERMINATE_FLOW',
                                           in_junction_weight_range_not='AS_IS',
                                           in_edge_weight_range_not='AS_IS')

    logging.info('Trace Complete. Checking counts and export.')
    group_layer = arcpy.mapping.Layer('in_memory\\geonet_trace_output')

    for layer in arcpy.mapping.ListLayers(group_layer):
        if not layer.isGroupLayer:
            layer_count = arcpy.GetCount_management(layer).getOutput(0)
            if int(layer_count) > 0:
                logging.info('found disconnected in layer: {}'.format(layer.name))
                logging.info('count disconnected: {}'.format(layer_count))

                arcpy.FeatureClassToFeatureClass_conversion(in_features=layer,
                                                            out_path=os.path.join(em.path, 'Routing', fgdb_name),
                                                            out_name=layer.name + '_disconnected_ALL')

    
    if arcpy.Exists(os.path.join(em.path, 'Routing', fgdb_name, 'ROAD_disconnected_ALL')):

        logging.info('Export ROAD disconnected UNNAMED')
        arcpy.FeatureClassToFeatureClass_conversion(in_features=os.path.join(em.path, 'Routing', fgdb_name, 'ROAD_disconnected_ALL'),
                                                    out_path=os.path.join(em.path, 'Routing', fgdb_name),
                                                    out_name='ROAD_disconnected_UNNAMED',
                                                    where_clause="ROAD_NAME = 'UNNAMED'")
        
        logging.info('Export ROAD disconnected NAMED Victoria')
        arcpy.FeatureClassToFeatureClass_conversion(in_features=os.path.join(em.path, 'Routing', fgdb_name, 'ROAD_disconnected_ALL'),
                                                    out_path=os.path.join(em.path, 'Routing', fgdb_name),
                                                    out_name='ROAD_disconnected_NAMED_VIC',
                                                    where_clause='''ROAD_NAME <> 'UNNAMED'
                                                        and (
                                                             (LEFT_LOCALITY not like '%(NSW)%' or
                                                              RIGHT_LOCALITY not like '%(NSW)%') AND
                                                             (LEFT_LOCALITY not like '%(SA)%' or
                                                              RIGHT_LOCALITY not like '%(SA)%')
                                                             )
                                                    ''')

        logging.info('Export ROAD disconnected NAMED Interstate')
        arcpy.FeatureClassToFeatureClass_conversion(in_features=os.path.join(em.path, 'Routing', fgdb_name, 'ROAD_disconnected_ALL'),
                                                    out_path=os.path.join(em.path, 'Routing', fgdb_name),
                                                    out_name='ROAD_disconnected_NAMED_INTERSTATE',
                                                    where_clause='''ROAD_NAME <> 'UNNAMED'
                                                        and (
                                                             (LEFT_LOCALITY like '%(NSW)%' or
                                                              RIGHT_LOCALITY like '%(NSW)%') OR
                                                             (LEFT_LOCALITY like '%(SA)%' or
                                                              RIGHT_LOCALITY like '%(SA)%')
                                                             )
                                                    ''')

    logging.info('clean up trace group layer')
    arcpy.Delete_management('in_memory\\geonet_trace_output')


def import_transport_disconnected(estamap_version):

    logging.info('environment')
    em = gis.ESTAMAP(estamap_version)

    if arcpy.Exists(os.path.join(em.path, 'Routing', 'TransportSpatialValidation_withPatch.gdb', 'ROAD_disconnected_ALL')):
        logging.info('importing ROAD_disconnected_ALL as ROAD_DISCONNECTED')
        if arcpy.Exists(os.path.join(em.sde, 'ROAD_DISCONNECTED')):
            arcpy.Delete_management(os.path.join(em.sde, 'ROAD_DISCONNECTED'))
        arcpy.FeatureClassToFeatureClass_conversion(in_features=os.path.join(em.path, 'Routing', 'TransportSpatialValidation_withPatch.gdb', 'ROAD_disconnected_ALL'),
                                                    out_path=em.sde,
                                                    out_name='ROAD_DISCONNECTED')
        road_disconnected_count = arcpy.GetCount_management(os.path.join(em.sde, 'ROAD_DISCONNECTED')).getOutput(0)
        logging.info('ROAD disconnected count: {}'.format(road_disconnected_count))

    if arcpy.Exists(os.path.join(em.path, 'Routing', 'TransportSpatialValidation_withPatch.gdb', 'ROAD_INFRASTRUCTURE_disconnected_ALL')):
        logging.info('importing ROAD_INFRASTRUCTURE_disconnected_ALL as ROAD_INFRASTRUCTURE_DISCONNECTED')
        if arcpy.Exists(os.path.join(em.sde, 'ROAD_INFRASTRUCTURE_DISCONNECTED')):
            arcpy.Delete_management(os.path.join(em.sde, 'ROAD_INFRASTRUCTURE_DISCONNECTED'))
        arcpy.FeatureClassToFeatureClass_conversion(in_features=os.path.join(em.path, 'Routing', 'TransportSpatialValidation_withPatch.gdb', 'ROAD_INFRASTRUCTURE_disconnected_ALL'),
                                                    out_path=em.sde,
                                                    out_name='ROAD_INFRASTRUCTURE_DISCONNECTED')
        road_infrastructure_disconnected_count = arcpy.GetCount_management(os.path.join(em.sde, 'ROAD_INFRASTRUCTURE_DISCONNECTED')).getOutput(0)
        logging.info('ROAD_INFRASTRUCTURE disconnected count: {}'.format(road_infrastructure_disconnected_count))


def transport_aspatial_validation(estamap_version):

    logging.info('environment')
    em = gis.ESTAMAP(estamap_version)


    logging.info('finding starting UFI')
    cursor = em.conn.cursor()
    starting_node = cursor.execute("select TOP 1 UFI from ROAD_INFRASTRUCTURE order by shape.STDistance(geometry::STGeomFromText('POINT (2497133.064  2409284.931)', 3111))").fetchval()
    #2313747
    logging.info('starting UFI: {}'.format(starting_node))
    

    logging.info('create table: ROAD_VALIDATION_DISCONNECTED')
    sql_script = os.path.join(em.path, 'sql', 'transport', 'create_road_validation_disconnected.sql')
    logging.info('running sql script: {}'.format(sql_script))
    dbpy.exec_script(em.server, em.database_name, sql_script)


    logging.info('create table: ROAD_VALIDATION_NETWORKED')
    sql_script = os.path.join(em.path, 'sql', 'transport', 'create_road_validation_networked.sql')
    logging.info('running sql script: {}'.format(sql_script))
    dbpy.exec_script(em.server, em.database_name, sql_script)

    logging.info('creating graph')
    graph = nx.Graph()

    logging.info('loading nodes')
    with arcpy.da.SearchCursor(in_table=os.path.join(em.sde, 'ROAD_INFRASTRUCTURE'),
                               field_names=['UFI',]) as sc:
        for enum, ufi in enumerate(sc):
            graph.add_node(ufi)
            if enum % 10000 == 0:
                logging.info(enum)
        logging.info(enum)

    logging.info('loading edges')
    with arcpy.da.SearchCursor(in_table=os.path.join(em.sde, 'ROAD'),
                               field_names=['PFI', 'FROM_UFI', 'TO_UFI']) as sc:
        for enum, (pfi, from_ufi, to_ufi) in enumerate(sc):
            graph.add_edge(int(from_ufi), int(to_ufi))
            # add PFI to the edge
            graph[int(from_ufi)][int(to_ufi)][int(pfi)] = True
            if enum % 10000 == 0:
                logging.info(enum)
        logging.info(enum)
    
    logging.info('appending patch')
    with arcpy.da.SearchCursor(in_table=os.path.join(em.sde, 'ROAD_PATCH'),
                               field_names=['FROM_UFI', 'TO_UFI'],
                               where_clause='MERGE_TRANSPORT = 1') as sc:
        for enum, (from_ufi, to_ufi) in enumerate(sc):
            graph.add_edge(int(from_ufi), int(to_ufi))
            # add PFI as -1 to the edge
            graph[int(from_ufi)][int(to_ufi)][-1] = True
        logging.info(enum)

    logging.info('finding connected')
    connected_nodes = nx.node_connected_component(graph, starting_node)
    graph_connected = graph.subgraph(connected_nodes)

    logging.info('finding disconnected')
    graph_disconnected = graph.fresh_copy()
    graph_disconnected.remove_nodes_from(connected_nodes)
    
    with dbpy.SQL_BULK_COPY(em.server, em.database_name, 'dbo.ROAD_VALIDATION_NETWORKED') as sbc_networked, \
         dbpy.SQL_BULK_COPY(em.server, em.database_name, 'dbo.ROAD_VALIDATION_DISCONNECTED') as sbc_disconnected:

        logging.info('looping network components...')
        edges_loaded = 0
        for enum_component, subnodes in enumerate(nx.connected_components(graph)):
            subgraph = graph.subgraph(subnodes)

            if starting_node in subgraph:
                # CONNECTED to starting node
                for u, v, d in subgraph.edges(data=True):
                    for enum, pfi in enumerate(d.keys()):
                        sbc_networked.add_row((pfi,))

                        edges_loaded = edges_loaded + 1
                        if edges_loaded % 10000 == 0:
                            sbc_networked.flush()
                            logging.info(edges_loaded)
            else:
                # DISCONNECTED to starting node
                for u, v, d in subgraph.edges(data=True):
                    for pfi in d.keys():
                        sbc_disconnected.add_row((pfi,))

                        edges_loaded = edges_loaded + 1
                        if edges_loaded % 10000 == 0:
                            sbc_disconnected.flush()
                            logging.info(edges_loaded)
        logging.info(edges_loaded)


def export_transport_validated(estamap_version):

    logging.info('environment')
    em = gis.ESTAMAP(estamap_version)
    conn = dbpy.create_conn_pyodbc(em.server, em.database_name)

    if arcpy.Exists(os.path.join(em.sde, 'ROAD_VALIDATED')):
        logging.info('deleting existing ROAD_VALIDATED')
        arcpy.Delete_management(os.path.join(em.sde, 'ROAD_VALIDATED'))
    if arcpy.Exists(os.path.join(em.sde, 'ROAD_INFRASTRUCTURE_VALIDATED')):
        logging.info('deleting existing ROAD_INFRASTRUCTURE_VALIDATED')
        arcpy.Delete_management(os.path.join(em.sde, 'ROAD_INFRASTRUCTURE_VALIDATED'))

    logging.info('make feature layer: ROAD')
    arcpy.MakeFeatureLayer_management(in_features=os.path.join(em.sde, 'ROAD'),
                                      out_layer='in_memory\\road_layer',
                                      where_clause="PFI not in (SELECT pfi from ROAD_DISCONNECTED)")

    logging.info('field mapping')
    fms = arcpy.FieldMappings()
    
    keep_fields = ['PFI',
                   'CLASS_CODE',
                   'FEATURE_TYPE_CODE',
                   'FROM_UFI',
                   'TO_UFI',
                   'LEFT_LOCALITY',
                   'RIGHT_LOCALITY',
                   ]
    for field in arcpy.ListFields(dataset='in_memory\\road_layer'):
##        print field.name, field.type
        if field.name in keep_fields:
            fm = arcpy.FieldMap()
            fm.addInputField('in_memory\\road_layer', field.name)
            fms.addFieldMap(fm)


    logging.info('exporting ROAD_VALIDATED')
    arcpy.FeatureClassToFeatureClass_conversion(in_features='in_memory\\road_layer',
                                                out_path=em.sde,
                                                out_name='ROAD_VALIDATED',
                                                field_mapping=fms)


    logging.info('make feature layer: ROAD_INFRASTRUCTURE')
##    arcpy.MakeFeatureLayer_management(in_features=os.path.join(em.sde, 'ROAD_INFRASTRUCTURE'),
##                                      out_layer='in_memory\\road_infra_layer',
##                                      where_clause="""UFI in (
##        SELECT R.FROM_UFI AS UFI FROM ROAD R INNER JOIN ROAD_VALIDATED RV  ON R.PFI = RV.PFI
##        UNION
##        SELECT R.TO_UFI AS UFI FROM ROAD R INNER JOIN ROAD_VALIDATED RV  ON R.PFI = RV.PFI  
##        )
##        """)
    arcpy.MakeFeatureLayer_management(in_features=os.path.join(em.sde, 'ROAD_INFRASTRUCTURE'),
                                      out_layer='in_memory\\road_infra_layer',
                                      where_clause="UFI not in (SELECT UFI FROM ROAD_INFRASTRUCTURE_DISCONNECTED)")

    logging.info('exporting ROAD_INFRASTRUCTURE_VALIDATED')
    arcpy.FeatureClassToFeatureClass_conversion(in_features='in_memory\\road_infra_layer',
                                                out_path=em.sde,
                                                out_name='ROAD_INFRASTRUCTURE_VALIDATED')


        
if __name__ == '__main__':

    sys.argv.append('--estamap_version=DEV')

    with log.LogConsole():
        
        logging.info('parsing args')
        args = docopt(__doc__)

        logging.info('variables')
        estamap_version = args['--estamap_version']
        log_file = args['--log_file']
        log_path = args['--log_path']

        with log.LogFile(log_file, log_path):
            logging.info('start')
            try:
                ###########
                
##                import_road_patch(estamap_version)
##
##                # initial validation
##                transport_spatial_validation(estamap_version, with_patch=False)
##
##                # validate with road patches applied
##                transport_spatial_validation(estamap_version, with_patch=True)
##
##                # import disconnected
##                import_transport_disconnected(estamap_version)
##               
##                # aspatial validation
##                transport_aspatial_validation(estamap_version)
##
                # export validated road and road_infra layers
                export_transport_validated(estamap_version)

                                    
                    
                
                ###########   
            except Exception as err:
                logging.exception('error occured running function.')
                raise
            logging.info('finished')

