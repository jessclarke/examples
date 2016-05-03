#-------------------------------------------------------------------------------
# Name:        sptlqry
# Purpose:     To provide an automated reporting process for a variety of shape
#              types and report types
#
# Author:      Jessica Clarke
#
# Created:     27 June 2013
# Copyright:   (c) Forestry Tasmania 2013
# Licence:     <your licence>
#
# Please use debug() to write test output, and trace() to write output for
# mcpd daemon to display.  Set debug() to False when in production
#-------------------------------------------------------------------------------
import cx_Oracle
import os, datetime, pprint
from HTMLParser import HTMLParser
from collections import defaultdict
from time import gmtime, strftime
from osgeo import ogr, osr
from sys import argv

#arguments from the command line (mapcomposer queue):
script, shape_id, shape_type, report_type, job_id = argv

def main():
##    centroid = getCentroid(shape_id, shape_type)
##    debug(centroid)
    date_time, day = getDateAndTime()
    report_list = getReportQueryList(report_type)
    debug(report_list)
    report_info = getReportInfo(report_type, report_list[0])
    debug(report_info)
    shape_info, centroid, orig_buffer = getShapeInfo(shape_id, shape_type)
    debug(shape_info)

    result_dict={} #dictionary that holds JUST the rows from row_dict{}
    result_to_print = [] #list that holds the complete results and followup
    #add this to the loop, yo.
    if report_type == 'SVCHEK':
        for row in report_list:
            #debug(row)
            row = setResultRowFormat(row)
            row_dict = setRowDict(row)
            check_codes.append(row['q_code'])
            result_info = row['result_info'].split(';')
            result_objs = row['result_obj'].split(';')
            #debug(row_dict)
            buffer_list = setBufferList(orig_buffer, row['targ_buff'])
            for buff in buffer_list:
                query_result = runQuery(row, buff, shape_info)
                result = processResult(query_result)

                if not result or result == '-' or result == '0':
                    #no results, so grab the negative options
                    result_dict['result_info'] = result_info[1]
                    result = result_objs[1]
                    debug('query result is negative')

                else:
                    result_dict['result_info'] = result_info[0]
                    follow_up_list.append([row['follow_up'], row['q_code']])
                    debug('query result is positive')
        setHeader(report_info, check_codes, shape_id, centroid, date_time, day, False)
        setSummaryBody(report_info, result_to_print, buffer_list, follow_up_list)
        setFooter(report_info)

    elif report_type == 'CONSRV':
        for row in report_list:
            row = setResultRowFormat(row)
            row_dict = setRowDict(row)
            result_info = row['result_info'].split(';')
            result_objs = row['result_obj'].split(';')

            debug(row_dict)

            #to create the column headings, and count how many columns there are
            columns = row['result_obj'].split(';')
            columns = columns[0].split(',')
            query_result = runQuery(row, row['targ_buff'], shape_info)
            debug(query_result)
            result = processResult(query_result)
            if not result or result == '-' or result == '0':
                #no results, so grab the negative options
                result_dict['result_info'] = result_info[1]
                result = result_objs[1]
                debug('query result is negative')

            else:
                result_dict['result_info'] = result_info[0]
                follow_up_list.append([row['follow_up'], row['q_code']])
                debug('query result is positive')
            if row['q_code']=='RNGFAU' or row['q_code']=='NVABDY':
                consrv_dict = setRowDict(row)
                consrv_dict = {'q_code':row['q_code'], 'q_name': row['q_name'], 'result': query_result, 'columns':columns}
            else:
                row_dict= {'q_code':row['q_code'], 'q_name': row['q_name'], 'result': query_result, 'columns':columns}
                result_to_print.append(row_dict)
        debug('the consrv header is %s'%consrv_dict)
        setHeader(report_info, check_codes, shape_id, centroid, date_time, day, consrv_dict)
        setDetailedBody(report_info, result_to_print)

    debug('the result_to_print is %s'%result_to_print)
    setFooter(report_info)
    report_file.close()

##    elif report_type == 'OPNPLA' or report_type == 'FIRMGT' or report_type == 'ADHOC' or report_type == 'PROPTY':

##    else:
##        error('The report type does not exist.')
def setHeader(report_info, check_codes, shape_id, centroid, date, day, additional_table_data):
    report_file.write("""<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Strict//EN" "http://www.w3.org/TR/xhtml1-strict.dtd">
        <!-- when creating a new header file, do not change the html and style below.  please proceed to the next comment -->
        <html>
        <head> <style type = "text/css">
        """)
    css = file(format_dir+report_info[5], "r")
    css_contents = css.read()
    for line in css_contents:
        report_file.write(line)
    report_file.write("""</style></head>""")
    css.close()

    #set coords to easting and northing values
    debug(centroid)
    grid_xy = str(centroid).strip('[(').strip(')]')
    grid_xy = grid_xy.split(',')
    #grab the values before the decimal point so that the
    #easting and northing values are accurate to the meter
    easting = grid_xy[0].split('.')
    northing = grid_xy[1].split('.')

    #grab the formatted header. this is formatted for each report type
    trace('Now editing header information.')
    debug('Header_file: '+format_dir+report_info[3])
    header = file(format_dir+report_info[3], "r")
    contents = header.read()
    contents = contents.replace('%css', report_info[5])
    contents = contents.replace('%check', report_info[1])
    contents = contents.replace('%coords', 'E: '+ easting[0] +', N: '+northing[0])
    contents = contents.replace('%loctype', shape_type)
    contents = contents.replace('%loc', shape_id)
    contents = contents.replace('%day', day)
    contents = contents.replace('%date', date)
    for line in contents:
        report_file.write(line)

    if additional_table_data:
    ##if there is a conserve table header
        header_table = """<div>
	          <table class="bodytable">
                    <tr><th width="100%%" colspan=4 align="left" style="background-color: grey; font-width: bold;">Threatened Fauna Range Boundaries</th></tr>
        			<tr  style="background-color: white; font-width: bold;">
        				<th width="15%%" valign="middle" align="center">Common Name</th>
        				<th width="10%%" valign="middle" align="center">Range Type</th>
        				<th width="45%%" valign="middle" align="center">Potential Habitat Description</th>
        				<th width="30%%" valign="middle" align="center">Planner Notes</th>
        			</tr>"""
        report_file.write(header_table)
        for result in additional_table_data['result']:
            debug(result)
            row_html = """<tr>
                        <td width="15%%" valign="middle" align="center" >%s</td>
                        <td width="10%%" valign="middle" align="center">%s</td>
                        <td width="45%%" valign="middle" align="justify">%s</td>
                        <td width="30%%" valign="middle" align="center">&nbsp;</td>
                    </tr>
                    """%(result[0], result[1], result[2])
            report_file.write(row_html)
        header_end = """</table></div>"""
        report_file.write(header_end)

    header.close()
    trace('Report header compilation complete. ')

def setSummaryBody(report_info, result_to_print, buffer_list, follow_up_list):
    trace('Compiling  summary report body. ')
    # a dictionary to sort the results into groups
    D = defaultdict(list)
    debug(follow_up_list)

    ##sorting the follow up list -- check if the code is there, if not, add
    ##it in the correct location
    action_dict = dict()
    for l in follow_up_list:
        debug('the follow up list: %s'%l)
        if l[0] in action_dict:
            debug('l[0] in the list %r'%l[0])
            if l[1] in action_dict[l[0]]:
                debug('l[1] in the list %r'%l[1])
            else:
                action_dict[l[0]].append(l[1].replace("[", "").replace("'", "").replace("]",""))
            debug('add the l[1] to the action dict: %r'%action_dict)
        else:
            action_dict[l[0]] = [l[1]]
            debug('set the l[0] marker to l[1] - %r'%action_dict)

    debug('the action dict %s'%action_dict)

    for item in result_to_print:
        D[item['q_cat_name']].append((item['q_name'], item['code'], item[0], item['def_buffer'],  item[1], item['follow_up'], item['q_desc'], item['result_info']))

     #sort the groups alphabetically, and print the column headers
    for item in sorted(D.items()):
        group_headers_html = """<tr><th colspan="5" width="100%%" valign="middle" align="left">%s</th></tr>"""%(item[0])
        report_file.write(group_headers_html)

        #sort the queries alphabetically, and print each row
        for i in sorted(item[1:len(item)]):
            for row in sorted(i):
                debug(row[1])
                rows_html ="""<tr>
                        <td>%s (%s)</td>
                        <td align="center">%s</td>
                        <td align="center">%s</td>
                        <td align="center">%s</td>
                        <td align="center">%s</td>
                    </tr>
                    """%(row[0], row[1], row[3], row[2], row[4], row[7])
                report_file.write(rows_html)
    tab_end_html = """</table></div>"""
    report_file.write(tab_end_html)

    #would suggest changing this logic to:
    #if footer.html contains '%followup', replace %followup with this process
    if report_info[0] == 'TORA' or 'SVCHEK':
        # include the follow up action table.
        fu_table_html = """<br><div>
        <table class="followuptable" cellspacing="0px" width="520px" >
            <tr><th colspan="4" width="100%%" align="middle" align="left">List of Follow Up Actions</th></tr>
            <tr>
                <th width="30%%" valign="middle" align="center">Follow Up Action</th>
                <th width="40%%" valign="middle" align="center">Comments</th>
                <th width="15%%" valign="middle" align="center">Completed By</th>
                <th width="15%%" valign="middle" align="center">Date Completed</th>
            </tr>"""
        report_file.write(fu_table_html)
        for list_item in action_dict:
            debug('This is the l value: %s'%list_item)
            fu_row_html = """<tr>
                <td width="30%%" valign="middle" align="left">%s - %s</td>
                <td width="40%%" valign="middle" align="center">&nbsp;</td>
                <td width="15%%" valign="middle" align="center">&nbsp;</td>
                <td width="15%%" valign="middle" align="center">&nbsp;</td>
            </tr>"""%(list_item, action_dict[list_item])
            report_file.write(fu_row_html)
        tab_end_html = """</table></div>"""
        report_file.write(tab_end_html)
    trace('Report body compilation complete. ')
    return None

def setDetailedBody(report_info, result_to_print):
    trace('Compiling detailed report body.')
    debug('result_to_print %s'%result_to_print)

    for item in result_to_print:
        #first query - key, [(result 1), (result 2),...(result n_)]
        start_table_html= """<div>
            <table class="bodytable">"""
        report_file.write(start_table_html)
        debug(item)
        key = item['q_name']
        result = item['result']

        columns = item['columns']
        num_columns = len(columns)
        debug(num_columns)

        group_headers_html = """
		<tr width="100%%"><th colspan=%s valign="middle" align="left">%s</th></tr>"""%(num_columns, key)
        report_file.write(group_headers_html)
        start_header_html = """<tr>"""
        report_file.write(start_header_html)
        for col in columns:
            debug(col)
            header_html = """
                <th>%s</th>"""%(col.replace('_', ' '))
            report_file.write(header_html)
        end_header_html = """</tr>"""
        report_file.write(end_header_html)
        #the entire row

        #for each row in the results
        for row in result:
            t = row
            row = list(t)
            start_row = """<tr>"""
            report_file.write(start_row)
            #for each value in the row, move across the columns
            for r in row:
                rows_html ="""<td align="center">%s </td>
                        """%(r)

                #check if there are no results, write across columns
                if r == '-':
                    r = 'There are no results for this area.'
                    rows_html = """
                        <td colspan=%s>%s</td>"""%(num_columns, r)
                report_file.write(rows_html)
        #end the row
            end_row = """</tr>"""
            report_file.write(end_row)
        tab_end_html = """</table></div><br/>"""
        report_file.write(tab_end_html)
    report_file.write("""</div>""")
    trace('Report body compilation complete. ')

def setFooter(report_info):
    trace('Compiling report footer. ')
    footer = file(format_dir+report_info[4], "r")
    contents = footer.read()
    contents = contents.replace('%report', report_info[1])
    for line in contents:
        report_file.write(line)
    report_file.write("</html>")
    trace('Report footer compilation complete. ')
    footer.close()

    return None

def connection(query, db):
    dbconnect_string = 'gis_ro/gislup@{0}'.format(db)
    conn = cx_Oracle.connect(dbconnect_string)
    try:
        cursor_main = cx_Oracle.Cursor(conn)
        cursor_main.execute(query)

        result = cursor_main.fetchall()
        cursor_main.close()
    except Exception as e:
        result = "Query Failed - %s" %e.message
    return result

def trace(message):
    """A trace method to assist with the trace messages for the daemon
    in the production version.  Simply check tracing boolean value, and print"""
    if tracing == True:
        now = datetime.datetime.now()
        date = now.strftime("%Y %m %d - %H:%M:%S")

        trace_file.write('%r %s\n'%(date, message))
        print date, 'sptlqry.py:', message

def debug(message):
    """A debugging method to assist with the processing of messages, and to
    destinguish the messages from formal trace messages"""
    if debugging == True:
        now = datetime.datetime.now()
        date = now.strftime("%d %b %Y - %H:%M:%S")
        debug_file.write('%r -- %r\n'%(date, message))
        print message

def error(self, message):
    pass

###--------------GETTERS---------------###
def getDateAndTime():
    now = datetime.datetime.now()
    day = now.strftime("%A")
    date_time = now.strftime("%d %B, %Y - %H:%M")
    return date_time, day

def getReportQueryList(report_type):
    reportQuery = """select *
            from gis.sq_group a
            join gis.sq_assoc_query_group b ON a.query_group_code = b.query_group_code
            join gis.sq_query c ON b.query_code = c.query_code
            join gis.lu_query_category d ON c.lu_query_category_code = d.lu_query_category_code
            join gis.lu_gis_data_source e ON c.lu_data_source_code = e.lu_data_source_code
            join gis.lu_gis_data_type f ON e.lu_data_type_code = f.lu_data_type_code
            join gis.lu_query_result_type g ON c.lu_result_type_code = g.lu_result_type_code
            WHERE a.query_group_code = '%s'"""%report_type
    reportList = connection(reportQuery, 'gisdev')
    return reportList

def getReportInfo(report_type, report_list):
    debug(report_list)

    global report_info
    report_info = report_list[0:7]  #get the first result row

    return report_info

def getShapeInfo(shape_id, shape_type):
    orig_buffer = '0' #zero unless otherwise specified, .1 accounts for touch atm. bit of a hack.
    if shape_type == 'COUPE' or shape_type == 'FOD':
        shape_info = setOracleShape(shape_id, shape_type)
##        relate_string = """from (select * from %s b, %s c where c.%s = '%s' AND SDO_RELATE(b.shape, SDO_GEOM.SDO_BUFFER(c.shape, %s, 1), 'MASK=ANYINTERACT')='TRUE') a"""%(data_source, shape[0], shape[1], shape_id, buff)
    elif shape_type == 'SHP':
        shape_info = setShapefileShape(shape_id)
    elif shape_type == 'COORDS' or shape_type == 'BBOX':
        shape_info, orig_buffer = setCoordShape(shape_id)
    centroid = getCentroid(shape_info, shape_id)

    return shape_info, centroid, orig_buffer
def getCentroid(shape_info, shape_id):

    grid_coords_qry = """select sdo_geom.sdo_centroid(shape, 0.5).sdo_point.x x, sdo_geom.sdo_centroid(shape, 0.5).sdo_point.Y y
            from %s where %s = '%s'"""%(shape_info[0], shape_info[1], shape_id)
    centroid = connection(grid_coords_qry, 'gisdb')
    print(centroid)
    return centroid

###--------------SETTERS--------------###
def setOracleShape(shape_id, shape_type):
    if shape_type == 'COUPE':
        shape_info = ['coupe.base_pc_a', 'provcoupe']
    elif shape_type == 'FOD':
        shape_info = ['fod.asset_a', 'asset_id']
    else:
        error('Invalid shape type at setOracleShape')
    return shape_info

def setShapefileShape(shape_id):
    #get the coord perimeters?
    # return shape_info
    pass

def setCoordShape(self, shape_id):
    shape_input = shape_id.split(',')
    orig_buffer=shape_input[2]
    return shape_info, orig_buffer


def setQueryType(result_type_code, data_source, result_obj, sel_criteria,  shape_info, buff):
#set the select strings according to query type
    if result_type_code == 'ATTRIB':
        result_objs = result_obj.split(';')
        debug(result_objs[0])
        result_objs = result_objs[0].replace(',', ',a.')

        select_string = """SELECT a.%s """%(result_objs)

    elif result_type_code == 'COUNT':
        select_string = """SELECT count(*) """

    elif result_type_code == 'CONST':
        select_string = """SELECT count(*) """

    else:
        error("Invalid result type code: %s " %result_type_code)

#if there is a sel_criteria value, this is intended to be a where clause
    if sel_criteria:
        where_string = """ WHERE a.%s"""%(sel_criteria)
    else:
        where_string = ''

    relate_string = setRelateString(data_source, shape_info, buff)
    query_string = """%s %s %s"""%(select_string, relate_string, where_string)
    debug(query_string)

    return query_string
def setRelateString(data_source, shape, buff):
    if shape_type == 'COUPE' or shape_type == 'FOD':
        relate_string = """from (select b.* from %s b, %s c where c.%s = '%s' AND SDO_RELATE(b.shape, SDO_GEOM.SDO_BUFFER(c.shape, %s, 1), 'MASK=ANYINTERACT')='TRUE') a"""%(data_source, shape[0], shape[1], shape_id, buff)
    elif shape_type == 'COORDS':
        relate_string = """from (select b.* from %s b where sdo_relate(b.shape, sdo_geom.sdo_buffer(sdo_geometry(2001, null, sdo_point_type(%s, %s, null), null, null), %s, 1), 'mask=anyinteract')='TRUE') a"""%(data_source, centroid_x, centroid_y, buff)
    elif shape_type == 'BBOX':
        relate_string = """from (Select b.* from %s b where sdo_relate(b.shape, sdo_geom.sdo_buffer(sdo_geometry(2003, NULL, NULL, SDO_ELEM_INFO_ARRAY(0, 1003, 3), SDO_ORDINATE_ARRAY (%s, %s, %s, %s)), %s, 1), 'mask=anyinteract')='TRUE') a"""(data_source, minx, miny, maxx, maxy, buff)
    else:
        error("Invalid Shape Type at setRelateString")
    return relate_string

def setBufferList(orig_buffer, targ_buffer):
     buffer_list=[orig_buffer, int(orig_buffer)+int(targ_buffer)]
     return buffer_list

def setRowDict(row):
    row_dict = {'code':row['q_code'],
                        'q_name': row['q_name'],
                        'q_desc': row['q_desc'],
                        'q_cat':row['q_cat_code'],
                        'q_cat_name':row['q_cat_name'],
                        'follow_up':row['follow_up'],
                        'def_buffer':row['targ_buff'],
                        'result_info':row['result_info']}
    return row_dict

def setResultRowFormat(row):
    """define the row so that it is easy to refer to throughout the script
    each row is in a unique dictionary with a unique key"""
    formatted_row = {
                'q_grp_code':row[0],
                'q_grp_name':row[1],
                'q_grp_desc':row[2],
                'rpt_header':row[3],
                'rpt_footer':row[4],
                'rpt_style':row[5],
                'rpt_format':row[6],
                'q_code':row[9],
                'q_name':row[10],
                'q_desc':row[11],
                'sel_criteria':row[12],
                'targ_buff':row[13],
                'follow_up':row[14],
                'result_obj':row[17],
                'mcmp_obj':row[19],
                'result_info':row[20],
                'q_cat_code':row[21],
                'q_cat_name':row[22],
                'q_cat_desc':row[23],
                'data_source_code':row[24],
                'data_source_name':row[25],
                'data_source_desc':row[26],
                'data_source':row[28],
                'data_type_code':row[29],
                'data_type_name':row[30],
                'data_type_desc':row[31],
                'result_type_code':row[32],
                'result_type_name':row[33],
                'result_type_desc':row[34]
                }
    debug('row: %r'%formatted_row)
    return formatted_row

###------------------DOERS------------###

def runQuery(row, buff, shape_info):
    if row['data_type_code'] == 'SHP':
        result = runShapeQuery(row, buff, shape_info)
        #result = runShapeQuery()
    elif row['data_type_code'] == 'ORA':
        result = runOracleQuery(row, buff, shape_info)
        #result = runOracleQuery(query_string)
    elif row['data_type_code'] == 'WFS':
       result = runWebFeatureServiceQuery()
    else:
        result = None
        error('Not yet implemented, or incorrect data type code')
    return result

def runShapeQuery(row, buff, shape_info):

    driver = ogr.GetDriverByName('ESRI Shapefile')
    tmp_ds = driver.CreateDataSource(r"C:\temp\tmp_shp_%s.shp"%job_id)

    if tmp_ds is None:
        debug('Could not open %s' %tmp_ds)
        exit(1)
    query_type = row['result_type_code'] #for the function definition
   # grid_xy, table = ora_shape.format_init_query(shape_id, shape_type, buff, row['data_source'])
    fn = row['data_source']
    #create temp location for the coordinates to query by

    try:
        if shape_type == 'COORDS':
            tmp_lyr = tmp_ds.CreateLayer('layer1',geom_type=ogr.wkbPoint)
            #create a point, with a buffer, and save as a shape file
            coord_xy = grid_xy[0]
            tmp_shape= ogr.Geometry(ogr.wkbPoint)
            tmp_shape.AddPoint_2D(int(centroid[0]), int(centroid[1]))
        else:
            tmp_lyr = tmp_ds.CreateLayer('layer1', geom_type=ogr.wkbPolygon)
            #create the query to get vertices, save as a shape file
            query_coords = """select b.x, b.y from %s a, TABLE(SDO_UTIL.GETVERTICES(a.shape)) b
                    where a.%s = '%s'"""%(shape_info[0], shape_info[1], shape_id) #gets all the coords
            shape_coords = connection(query_coords, 'gisdb')

            tmp_shape = ogr.Geometry(ogr.wkbPolygon)
            ring = ogr.Geometry(ogr.wkbLinearRing)
            #create the temp feature
            for pnt in shape_coords:
                ring.AddPoint(pnt[0], pnt[1])
            ring.CloseRings()
            tmp_shape.AddGeometry(ring)
    except:
        driver.DeleteDataSource(tmp_ds)
        tmp_lyr = tmp_ds.CreateLayer('layer1', geom_type_string)

    tmp_lyr_defn = tmp_lyr.GetLayerDefn()
    feature = ogr.Feature(tmp_lyr_defn)
    feature.SetGeometry(tmp_shape)

    tmp_lyr.CreateFeature(feature)
    buffer_geom = tmp_shape.Buffer(int(buff))

    #create the location of the layer to query
    qry_ds = driver.Open(fn, 0)
    debug('qry_ds open')
    if qry_ds is None:
        trace('Could not open the source shapefile: %s' %fn)
        result = 'Datasource could not be found: %s' %fn
    try:
        qry_lyr = qry_ds.GetLayer(0)
    except:
        tmp_ds.Destroy()
        os.remove(r"C:\temp\tmp_shp_%s.shp"%job_id)
        os.remove(r"C:\temp\tmp_shp_%s.dbf"%job_id)
        os.remove(r"C:\temp\tmp_shp_%s.shx"%job_id)
        debug('Destroyed dataSource')
        return 'qry_ds.GetLayer(0) failed.'
    qry_lyr_Defn = qry_lyr.GetLayerDefn()
    debug(qry_lyr.GetFeatureCount())
    #layer the coord layer over the query layer

    #process results
    qry_lyr.SetSpatialFilter(buffer_geom)
    debug(qry_lyr.GetFeatureCount())

    if query_type == 'CONST' or 'COUNT':
        debug('const or count')
        debug(row['sel_criteria'])
        if row['sel_criteria'] is None:
            sel_crit = 'SHAPE'
        else:
            debug('attrib or else')
            sel_crit = row['sel_criteria']
        try:
            qry_lyr.SetAttributeFilter(sel_crit)
            numAttr = qry_lyr.GetFeatureCount()
            debug('num_attr %s'%numAttr)
            result_objs = row['result_obj'].split(';')

            if numAttr > 0 and query_type == 'COUNT':
                result = numAttr
                follow_up_list.append([row['follow_up'], row['q_code']])
            elif numAttr > 0 and query_type=='CONST':
                result = result_objs[0]
                follow_up_list.append([row['follow_up'], row['q_code']])
            else:
                result = result_objs[1]

        except Exception as e:
            result = 'query failed: check selection criteria, or size of dataset'
            print e.message

    elif query_type == 'ATTRIB':
            attributes = row['result_obj']
            attr = attributes[0:-2].split(',')
            result = []
            feature = qry_lyr.GetNextFeature()
            for a in attr:
                try:
                    r = feature.GetFieldAsString(a)
                    result.append(r)
                    follow_up_list.append([row['follow_up'], row['q_code']])
                except Exception as e:
                    #no value found in the buffered area
                    debug('No values found due to %s'%e.message)
                    result = '-'
    tmp_ds.Destroy()
    qry_ds.Destroy()
    os.remove(r"C:\temp\tmp_shp_%s.shp"%job_id)
    os.remove(r"C:\temp\tmp_shp_%s.dbf"%job_id)
    os.remove(r"C:\temp\tmp_shp_%s.shx"%job_id)

    debug('The shape query result for %s - %s is %s '%(row['sel_criteria'], query_type, result))
    return [result]

def runOracleQuery(row, buff,shape_info):
    query_string = setQueryType(row['result_type_code'], row['data_source'], row['result_obj'], row['sel_criteria'], shape_info, buff)
    debug(query_string)

    if row['q_code']== 'NVABDY':
        result = connection(query_string, 'gistest')
    else:
        result = connection(query_string, 'gisdb')

    #result = connection(query_string, 'gisdb')
    debug(result)
    return result

def runWebFeatureServiceQuery(row, buff, shape_info):
    pass

def processResult(result_to_process):
     #process the result for the format of the report
    t = result_to_process
    result = list(t)
    result = (', '.join( repr(e) for e in result))
    result= result.replace('(', '').replace(',)','').replace("'", '')
    debug(result)
    return result


## ---------- init -------------- ##
#setup the class
##set to False to get rid of test traces
tracing = True
debugging = True
follow_up_list = []
check_codes = []

exec_dir = "C:\\mcpd\\compositions\\"
format_dir = exec_dir+"sq_text_files\\"

#create a new output file, based on the job_id from mapcomposer
report_filename = '%s_report.html' %job_id
report_file = file('C:\\plots\\'+report_filename, 'w')

#create a new trace file, based on the job_id from mapcomposer
trace_filename = '%s_trace.txt' %job_id
trace_file = file('C:\\plots\\'+trace_filename, 'w')

#create a new debug file stored in temp
debug_filename = 'debug.txt'
debug_file = file('C:\\temp\\'+ debug_filename, 'w')

#instantiate the class objects
#ora_conn = Oracle_Connections()
#ora_shape = Oracle_Shape()

#rpt = Report()
main()