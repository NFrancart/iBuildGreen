import psycopg as pg
import random as rd
import pandas as pd
from IPython.display import clear_output # Function to clear output when counting rows
        
# This is a rough attempt at putting some of the most important functions from the Jupyter notebooks into an importable package

class macrocomponent_database:
    def __init__(self,db_params,bbr_params,default_floor_height=3.5,window_wall_ratio=0.2,space_efficiency=1.2):
        self.db_params=db_params
        self.bbr_params=bbr_params
        self.default_floor_height=default_floor_height
        self.window_wall_ratio=window_wall_ratio
        self.space_efficiency=space_efficiency
        self.get_perimeter_sql=f"SELECT (CASE WHEN (b.byg054AntalEtager IS NULL OR b.byg054AntalEtager = 0) THEN SQRT(b.byg041BebyggetAreal)*2*(%s+1/%s) ELSE SQRT(b.byg038SamletBygningsareal/b.byg054AntalEtager)*2*(%s+1/%s) END) as perimeter" % (space_efficiency,space_efficiency,space_efficiency,space_efficiency) #Rough approximation if the building has storeys of different sizes
    
    # Generic function to run SQL queries
    def run_sql(self,SQLcode):
        connector=None
        try:
            # connect to the PostgreSQL database
            connector = pg.connect(self.db_params)

            # create a new cursor
            cur = connector.cursor()

            # execute the SQL statement
            cur.execute(SQLcode)

            # commit the changes to the database
            connector.commit()

            # close communication with the database
            cur.close()

        except (Exception, pg.DatabaseError) as error:
            print('error: '+str(error))

        finally:
            if connector is not None:
                connector.close()

    # Functions to insert BBR data into the database
    def insert_bbr_from_dict(self,row_dict):
        # We first convert the dictionary into a tuple, since the PostgresQL insertion function takes a tuple as input
        # The dictionary keys are the names of parameters from BBR, in Danish
        l=list()
        ks=row_dict.keys()
        for k in ks:
            l.append(row_dict[k])
        row_tuple=tuple(l)
        
        # Building the SQL query to insert values in the database.     
        sql ="INSERT INTO buildings("
        for k in ks:
            sql+=k+', '
        sql=sql[0:len(sql)-2]+') VALUES('
        for n in range(len(ks)):
            sql+='%s, '
        sql=sql[0:len(sql)-2]+') ON CONFLICT ON CONSTRAINT buildings_pkey DO UPDATE SET ('
        for k in ks:
            sql+=k+', '
        sql=sql[0:len(sql)-2]+') = ('
        for k in ks:
            sql+='EXCLUDED.'+k+', '
        sql=sql[0:len(sql)-2]+');'

        self.run_sql(sql)

    def retrieve_values(self,jsonfile,last_recorded_id=''):
        jsondata = open(jsonfile, encoding='utf8')
        items = ijson.kvitems(jsondata, 'BygningList.item')
        building_dict=dict()
        isNewBuilding=False # Have we already recorded this building in a previous (unfinished) run?
        
        if last_recorded_id=='':
            isNewBuilding=True # By default we assume that no building has previously been recorded.
        
        for param, value in items: # Parse the json file, reading the name and value of each parameter for each building
            
            if param == 'forretningshændelse': # This is the first parameter in the JSON file for each building, so it indicates the start of a new building.
                
                # Insert previously recorded values, if we have not recorded this building before:
                if len(building_dict.values())>0:
                    if isNewBuilding:
                        self.insert_bbr_from_dict(building_dict)         
                        
                        # Save the recorded building's id to start again from there in case the program crashes (use it as the last_recorded_id parameter for the next run).
                        last_building_recorded=building_dict['id_lokalId']
                        
                    # If the current id is equal to the last_recorded_id from a previous run, all buildings read after this point must be recorded
                    elif building_dict['id_lokalId']==last_recorded_id:
                        isNewBuilding=True
                    
                # Reset the building dictionary to record values for the next building:
                building_dict=dict()

            elif param in bbr_parameters:
                # If the parameter we're reading is on the list of parameters we're interested in, record it.
                building_dict[param]=value    

    # Functions to query building properties and material amounts from the database
    def properties_one_building(self,parameter_list,bbr_id):
        dic={}
        for p in parameter_list:
            dic[p]=[]
        index=[]
        
        SQL="SELECT "
        for p in parameter_list:
            SQL+=p+', '
        SQL=SQL[0:len(SQL)-2]
        SQL+=" FROM buildings WHERE id_lokalId=%s"
        
        conn=None

        try:
            conn = pg.connect(self.db_params, row_factory=pg.rows.dict_row)
            cur=conn.cursor()
            
            cur.execute(SQL,(bbr_id,))
            row=cur.fetchone()

            while row is not None:
                index.append(row['id_lokalid'])
                for k in dic.keys():
                    dic[k].append(row[k.lower()])
                    
                row=cur.fetchone()
                
            results=pd.DataFrame(dic,index)

            conn.commit()
            cur.close()
            
            return(results)

        except (Exception, pg.DatabaseError) as error:
            print('error: '+str(error))
        finally:
            if conn is not None:
                conn.close()  

    def properties_all_buildings(self,parameter_list):
        dic={}
        for p in parameter_list:
            dic[p]=[]
        index=[]
        
        SQL="SELECT "
        for p in parameter_list:
            SQL+=p+', '
        SQL=SQL[0:len(SQL)-2]
        SQL+=" FROM buildings"
        
        conn=None

        try:
            conn = pg.connect(self.db_params, row_factory=pg.rows.dict_row)
            cur=conn.cursor()
            
            cur.execute(SQL,(bbr_id,))
            row=cur.fetchone()

            while row is not None:
                index.append(row['id_lokalid'])
                for k in dic.keys():
                    dic[k].append(row[k.lower()])
                    
                row=cur.fetchone()
                
            results=pd.DataFrame(dic,index)

            conn.commit()
            cur.close()
            
            return(results)

        except (Exception, pg.DatabaseError) as error:
            print('error: '+str(error))
        finally:
            if conn is not None:
                conn.close() 

    def results_one_building(self,bbr_id):
        dic={'bbr_id':[],'element':[],'product':[],'weight':[],'material_type':[]}
        
        SQL="""
        SELECT
        rma.bbr_id bbr_id,
        rma.element element,
        rma.product product,
        weight,
        material_type
        FROM results_material_amounts rma
        INNER JOIN products pr ON rma.product=pr.name,
        LATERAL (SELECT (CASE WHEN unit='KG' THEN amount WHEN unit='M3' THEN amount*pr.density ELSE NULL END) AS weight) lt
        WHERE rma.bbr_id=%s
        ORDER BY element  
        """
        
        try:
            conn = pg.connect(self.db_params, row_factory=pg.rows.dict_row)
            cur=conn.cursor()
            
            cur.execute(SQL,(bbr_id,))
            row=cur.fetchone()     
                
            while row is not None:
                for k in dic.keys():
                    dic[k].append(row[k.lower()])

                row=cur.fetchone()
                
            results=pd.DataFrame(dic)
            
            conn.commit()
            cur.close()
            
            return(results)

        except (Exception, pg.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close() 

    def results_all_buildings(self):
        dic={'bbr_id':[],'element':[],'product':[],'weight':[],'material_type':[]}
        
        SQL="""
        SELECT
        rma.bbr_id bbr_id,
        rma.element element,
        rma.product product,
        weight,
        material_type
        FROM results_material_amounts rma
        INNER JOIN products pr ON rma.product=pr.name,
        LATERAL (SELECT (CASE WHEN unit='KG' THEN amount WHEN unit='M3' THEN amount*pr.density ELSE NULL END) AS weight) lt
        ORDER BY bbr_id  
        """
        
        try:
            conn = pg.connect(self.db_params, row_factory=pg.rows.dict_row)
            cur=conn.cursor()
            
            cur.execute(SQL)
            row=cur.fetchone()     
                
            while row is not None:
                for k in dic.keys():
                    dic[k].append(row[k.lower()])

                row=cur.fetchone()
                
            results=pd.DataFrame(dic)
            
            conn.commit()
            cur.close()
            
            return(results)

        except (Exception, pg.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close() 

    def material_amounts_one_building(self,bbr_id):
        dic={'bbr_id':[],'clay':[],'cement_mortar':[],'concrete':[],'gypsum_plaster':[], 'metal':[],'wood':[],'wool':[],'glass':[],'other':[]}
        index=[]
        
        SQL1="""
        CREATE TEMP TABLE IF NOT EXISTS agg_material (
            bbr_id varchar(50) PRIMARY KEY,
            Clay real,
            Cement_mortar real,
            Concrete real,
            Aggregates real,
            Gypsum_plaster real,
            Metal real,
            Wood real,
            Wool real,
            Glass real,
            Other real);"""
        
        SQL2="""
        WITH t AS(
        SELECT
        rma.bbr_id,
        weight,
        material_type
        FROM results_material_amounts rma
        INNER JOIN products pr ON rma.product=pr.name,
        LATERAL (SELECT (CASE WHEN unit='KG' THEN amount WHEN unit='M3' THEN amount*pr.density ELSE NULL END) AS weight) lt
        WHERE rma.bbr_id=%s
        )

        SELECT
        bbr_id,
        SUM(weight) weightsum,
        material_type
        INTO TEMPORARY TABLE t2
        FROM t
        GROUP BY material_type, bbr_id;    
        """

        SQLclay="""
        INSERT INTO agg_material(bbr_id, Clay)
        SELECT 
        bbr_id,
        weightsum
        FROM t2
        WHERE material_type = 'Clay'
        ON CONFLICT ON CONSTRAINT agg_material_pkey DO UPDATE SET (bbr_id, Clay) = (EXCLUDED.bbr_id, EXCLUDED.Clay);
        """
        
        SQLcement="""
        INSERT INTO agg_material(bbr_id, Cement_mortar)
        SELECT 
        bbr_id,
        weightsum
        FROM t2
        WHERE material_type = 'Cement_mortar' 
        ON CONFLICT ON CONSTRAINT agg_material_pkey DO UPDATE SET (bbr_id, Cement_mortar) = (EXCLUDED.bbr_id, EXCLUDED.Cement_mortar);
        """

        SQLconcrete="""
        INSERT INTO agg_material(bbr_id, Concrete)
        SELECT 
        bbr_id,
        weightsum
        FROM t2
        WHERE material_type = 'Concrete'
        ON CONFLICT ON CONSTRAINT agg_material_pkey DO UPDATE SET (bbr_id, Concrete) = (EXCLUDED.bbr_id, EXCLUDED.Concrete);
        """
        
        SQLaggregates="""
        INSERT INTO agg_material(bbr_id, Aggregates)
        SELECT 
        bbr_id,
        weightsum
        FROM t2
        WHERE material_type = 'Aggregates'
        ON CONFLICT ON CONSTRAINT agg_material_pkey DO UPDATE SET (bbr_id, Aggregates) = (EXCLUDED.bbr_id, EXCLUDED.Aggregates);
        """

        SQLgypsum="""
        INSERT INTO agg_material(bbr_id, Gypsum_plaster)
        SELECT 
        bbr_id,
        weightsum
        FROM t2
        WHERE material_type = 'Gypsum_plaster'
        ON CONFLICT ON CONSTRAINT agg_material_pkey DO UPDATE SET (bbr_id, Gypsum_plaster) = (EXCLUDED.bbr_id, EXCLUDED.Gypsum_plaster);
        """

        SQLmetal="""
        INSERT INTO agg_material(bbr_id, Metal)
        SELECT 
        bbr_id,
        weightsum
        FROM t2
        WHERE material_type = 'Metal'
        ON CONFLICT ON CONSTRAINT agg_material_pkey DO UPDATE SET (bbr_id, Metal) = (EXCLUDED.bbr_id, EXCLUDED.Metal);
        """

        SQLwood="""
        INSERT INTO agg_material(bbr_id, Wood)
        SELECT 
        bbr_id,
        weightsum
        FROM t2
        WHERE material_type = 'Wood'
        ON CONFLICT ON CONSTRAINT agg_material_pkey DO UPDATE SET (bbr_id, Wood) = (EXCLUDED.bbr_id, EXCLUDED.Wood);
        """

        SQLwool="""
        INSERT INTO agg_material(bbr_id, Wool)
        SELECT 
        bbr_id,
        weightsum
        FROM t2
        WHERE material_type = 'Wool'
        ON CONFLICT ON CONSTRAINT agg_material_pkey DO UPDATE SET (bbr_id, Wool) = (EXCLUDED.bbr_id, EXCLUDED.Wool);
        """
        
        SQLglass="""
        INSERT INTO agg_material(bbr_id, Glass)
        SELECT 
        bbr_id,
        weightsum
        FROM t2
        WHERE material_type = 'Glass'
        ON CONFLICT ON CONSTRAINT agg_material_pkey DO UPDATE SET (bbr_id, Glass) = (EXCLUDED.bbr_id, EXCLUDED.Glass);
        """

        SQLother="""
        INSERT INTO agg_material(bbr_id, Other)
        SELECT 
        bbr_id,
        weightsum
        FROM t2
        WHERE material_type = 'Other'
        ON CONFLICT ON CONSTRAINT agg_material_pkey DO UPDATE SET (bbr_id, Other) = (EXCLUDED.bbr_id, EXCLUDED.Other);
        """

        SQLselect="SELECT * FROM agg_material"
        
        try:
            conn = pg.connect(self.db_params, row_factory=pg.rows.dict_row)
            cur=conn.cursor()
            
            cur.execute(SQL1)
            cur.execute(SQL2,(bbr_id,))
            cur.execute(SQLclay)
            cur.execute(SQLcement)
            cur.execute(SQLconcrete)
            cur.execute(SQLaggregates)
            cur.execute(SQLgypsum)
            cur.execute(SQLmetal)
            cur.execute(SQLwood)
            cur.execute(SQLwool)
            cur.execute(SQLglass)
            cur.execute(SQLother)
            cur.execute(SQLselect)       

            row=cur.fetchone()
            
            while row is not None:
                index.append(row['bbr_id'])
                for k in dic.keys():
                    dic[k].append(row[k.lower()])

                row=cur.fetchone()
                
            results=pd.DataFrame(dic,index)

            conn.commit()
            cur.close()
            
            return(results)

        except (Exception, pg.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close() 

    def material_amounts_all(self):
        dic={'bbr_id':[],'clay':[],'cement_mortar':[],'concrete':[],'gypsum_plaster':[], 'metal':[],'wood':[],'wool':[],'other':[]}
        index=[]
        
        SQL1="""
        CREATE TEMP TABLE IF NOT EXISTS agg_material (
            bbr_id varchar(50) PRIMARY KEY,
            Clay real,
            Cement_mortar real,
            Concrete real,
            Aggregates real,
            Gypsum_plaster real,
            Metal real,
            Wood real,
            Wool real,
            Glass real,
            Other real);"""
        
        SQL2="""
        WITH t AS(
        SELECT
        rma.bbr_id,
        weight,
        material_type
        FROM results_material_amounts rma
        INNER JOIN products pr ON rma.product=pr.name,
        LATERAL (SELECT (CASE WHEN unit='KG' THEN amount WHEN unit='M3' THEN amount*pr.density ELSE NULL END) AS weight) lt
        )

        SELECT
        bbr_id,
        SUM(weight) weightsum,
        material_type
        INTO TEMPORARY TABLE t2
        FROM t
        GROUP BY material_type, bbr_id;    
        """

        SQLclay="""
        INSERT INTO agg_material(bbr_id, Clay)
        SELECT 
        bbr_id,
        weightsum
        FROM t2
        WHERE material_type = 'Clay'
        ON CONFLICT ON CONSTRAINT agg_material_pkey DO UPDATE SET (bbr_id, Clay) = (EXCLUDED.bbr_id, EXCLUDED.Clay);
        """
        
        SQLcement="""
        INSERT INTO agg_material(bbr_id, Cement_mortar)
        SELECT 
        bbr_id,
        weightsum
        FROM t2
        WHERE material_type = 'Cement_mortar' 
        ON CONFLICT ON CONSTRAINT agg_material_pkey DO UPDATE SET (bbr_id, Cement_mortar) = (EXCLUDED.bbr_id, EXCLUDED.Cement_mortar);
        """

        SQLconcrete="""
        INSERT INTO agg_material(bbr_id, Concrete)
        SELECT 
        bbr_id,
        weightsum
        FROM t2
        WHERE material_type = 'Concrete'
        ON CONFLICT ON CONSTRAINT agg_material_pkey DO UPDATE SET (bbr_id, Concrete) = (EXCLUDED.bbr_id, EXCLUDED.Concrete);
        """
        
        SQLaggregates="""
        INSERT INTO agg_material(bbr_id, Aggregates)
        SELECT 
        bbr_id,
        weightsum
        FROM t2
        WHERE material_type = 'Aggregates'
        ON CONFLICT ON CONSTRAINT agg_material_pkey DO UPDATE SET (bbr_id, Aggregates) = (EXCLUDED.bbr_id, EXCLUDED.Aggregates);
        """

        SQLgypsum="""
        INSERT INTO agg_material(bbr_id, Gypsum_plaster)
        SELECT 
        bbr_id,
        weightsum
        FROM t2
        WHERE material_type = 'Gypsum_plaster'
        ON CONFLICT ON CONSTRAINT agg_material_pkey DO UPDATE SET (bbr_id, Gypsum_plaster) = (EXCLUDED.bbr_id, EXCLUDED.Gypsum_plaster);
        """

        SQLmetal="""
        INSERT INTO agg_material(bbr_id, Metal)
        SELECT 
        bbr_id,
        weightsum
        FROM t2
        WHERE material_type = 'Metal'
        ON CONFLICT ON CONSTRAINT agg_material_pkey DO UPDATE SET (bbr_id, Metal) = (EXCLUDED.bbr_id, EXCLUDED.Metal);
        """

        SQLwood="""
        INSERT INTO agg_material(bbr_id, Wood)
        SELECT 
        bbr_id,
        weightsum
        FROM t2
        WHERE material_type = 'Wood'
        ON CONFLICT ON CONSTRAINT agg_material_pkey DO UPDATE SET (bbr_id, Wood) = (EXCLUDED.bbr_id, EXCLUDED.Wood);
        """

        SQLwool="""
        INSERT INTO agg_material(bbr_id, Wool)
        SELECT 
        bbr_id,
        weightsum
        FROM t2
        WHERE material_type = 'Wool'
        ON CONFLICT ON CONSTRAINT agg_material_pkey DO UPDATE SET (bbr_id, Wool) = (EXCLUDED.bbr_id, EXCLUDED.Wool);
        """
        
        SQLglass="""
        INSERT INTO agg_material(bbr_id, Glass)
        SELECT 
        bbr_id,
        weightsum
        FROM t2
        WHERE material_type = 'Glass'
        ON CONFLICT ON CONSTRAINT agg_material_pkey DO UPDATE SET (bbr_id, Glass) = (EXCLUDED.bbr_id, EXCLUDED.Glass);
        """

        SQLother="""
        INSERT INTO agg_material(bbr_id, Other)
        SELECT 
        bbr_id,
        weightsum
        FROM t2
        WHERE material_type = 'Other'
        ON CONFLICT ON CONSTRAINT agg_material_pkey DO UPDATE SET (bbr_id, Other) = (EXCLUDED.bbr_id, EXCLUDED.Other);
        """

        SQLselect="SELECT * FROM agg_material"
        
        try:
            conn = pg.connect(self.db_params, row_factory=pg.rows.dict_row)
            cur=conn.cursor()
            
            cur.execute(SQL1)
            cur.execute(SQL2)
            cur.execute(SQLclay)
            cur.execute(SQLcement)
            cur.execute(SQLconcrete)
            cur.execute(SQLaggregates)
            cur.execute(SQLgypsum)
            cur.execute(SQLmetal)
            cur.execute(SQLwood)
            cur.execute(SQLwool)
            cur.execute(SQLglass)
            cur.execute(SQLother)
            cur.execute(SQLselect)       

            row=cur.fetchone()
            
            while row is not None:
                index.append(row['bbr_id'])
                for k in dic.keys():
                    dic[k].append(row[k.lower()])

                row=cur.fetchone()

            results=pd.DataFrame(dic,index)
                
            conn.commit()
            cur.close()
            
            return(results)

        except (Exception, pg.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close() 

    # Functions to associate buildings with macrocomponents
    def random_possible_element (self, elem_list, id_list, construction_year): 
        # From a list of possible solutions, picks a random one based on building construction year
        # elem_list is the list of all  macrocomponents for one particular building part (e.g. external walls). It is a list of tuples that will later be retrieved from the database.
        # The first element of each tuple is the macrocomponent id and the 3rd and 4th are min and max years of use for this component.
        # id_list is the list of element ids for possible elements. We can narrow down the list of possible elements based on information from BBR regarding facade and roof cover.
        
        if construction_year == None:
            return None
        else:
            valid_choices=[] # list of macrocomponents where the building's construction year is between the component's min and max years of use.
            early_choices=[] # list of macrocomponents where the building's construction year is before the component's min year
            late_choices=[]  # list of macrocomponents where the building's construction year is after the component's max year
            
            for elem in elem_list:
                elem_id=elem[0]
                min_year=elem[2]
                max_year=elem[3]
                if elem_id in id_list and min_year<=construction_year<=max_year:
                    valid_choices.append(elem)
                elif elem_id in id_list and max_year<construction_year:
                    late_choices.append(elem)
                elif elem_id in id_list and construction_year<min_year:
                    early_choices.append(elem)
                    
            if len(valid_choices)!=0:
                random_solution = rd.choice(valid_choices)   # if there are valid choices, pick one of them
                return random_solution 
            elif len(late_choices)!=0:
                random_solution = rd.choice(late_choices)    # otherwise, return one of the late choices if any
                return random_solution
            elif len(early_choices)!=0:
                random_solution = rd.choice(early_choices)   # otherwise, return one of the early choices if any
                return random_solution
            else:
                print('no valid choice')
                return None
            
    def get_ext_wall(self, elems, bbr_material, cyear): 
        valid_elements = []
        for e in elems:
            if bbr_material in e[-1]:
                valid_elements.append(e[0]) # Make a list of all wall types that fit the BBR code
        return self.random_possible_element(elems,valid_elements,cyear) # Pick one of these at random

    def link_ext_walls(self):
        conn=None
        try:
            conn = pg.connect(self.db_params)
            cur_elem=conn.cursor()
            cur_elem.execute("SELECT * FROM ext_wall_types ORDER BY id")
            elems = cur_elem.fetchall() # Retrieve the list of all external wall types, which can be fed to the random choice function
            # Make sure that bbr_material_id is the last column of the ext_walls table in the database - if not, adjust the SQL query to make sure that it returns a row where bbr_material_id is the last item
            
            cur = conn.cursor()
            cur.execute("DELETE FROM buildings_to_ext_walls")
            cur.execute("SELECT id_lokalid, byg032ydervæggensmateriale, byg026opførelsesår, byg021bygningensanvendelse FROM buildings")
            row = cur.fetchone() # Get properties from the first building as a tuple
                
            cur_write=conn.cursor()
            row_number=0

            while row is not None:
                row_number+=1
                clear_output(wait=True)
                print(row_number)
                bbr_material=row[1] # Get reported wall material for the building
                cyear=row[2] # Get the building's construction year
                ext_wall=self.get_ext_wall(elems, bbr_material, cyear) # Pick a suitable type of external wall for the building
                if ext_wall is not None:
                    # Add entry to the mapping table
                    cur_write.execute("INSERT INTO buildings_to_ext_walls(bbr_id, ext_wall_id) VALUES (%s, %s)", (row[0], ext_wall[0]))
                else:
                    # If there is no valid choice, add NULL to the mapping table
                    cur_write.execute("INSERT INTO buildings_to_ext_walls(bbr_id) VALUES (%s)", (row[0],))
                
                row=cur.fetchone() # Retrieve the next building as a tuple and iterate

            conn.commit()
            cur_write.close()
            cur_elem.close()
            cur.close()

        except (Exception, pg.DatabaseError) as error:
            print('row number'+str(row_number)+'error: ')
            print(error)
        finally:
            if conn is not None:
                conn.close()

    def get_roof_cover(self, elems, bbr_material, cyear): # No category for shingles in BBR?
        valid_elements = []
        for e in elems:
            if bbr_material in e[-1]:
                valid_elements.append(e[0])
        return self.random_possible_element(elems,valid_elements,cyear)

    def link_roof_cover(self):
        conn=None
        try:
            conn = pg.connect(self.db_params)
            cur_elem=conn.cursor()
            cur_elem.execute("SELECT * FROM roof_cover_types ORDER BY id")
            elems = cur_elem.fetchall() # Retrieve the list of all roof cover types, which can be fed to the random choice function

            cur = conn.cursor()
            cur.execute("DELETE FROM buildings_to_roof_covers")    
            cur.execute("SELECT id_lokalid, byg033tagdækningsmateriale, byg026opførelsesår, byg021bygningensanvendelse FROM buildings")
            row = cur.fetchone() # Get properties from the first building as a tuple
                
            cur_write=conn.cursor()
            row_number=0

            while row is not None:
                row_number+=1
                clear_output(wait=True)
                print(row_number)
                bbr_material=row[1] # Get reported roof cover material for the building
                cyear=row[2] # Get the building's construction year
                roof_cover=self.get_roof_cover(elems, bbr_material, cyear)
                if roof_cover is not None:
                    # Add entry to the mapping table
                    cur_write.execute("INSERT INTO buildings_to_roof_covers(bbr_id, roof_cover_id) VALUES (%s, %s)", (row[0], roof_cover[0]))
                else:
                    # If there is no valid choice, add None to the mapping table
                    cur_write.execute("INSERT INTO buildings_to_roof_covers(bbr_id) VALUES (%s)", (row[0],))
                
                row=cur.fetchone() # Retrieve the next building as a tuple and iterate

            conn.commit()
            cur_write.close()
            cur_elem.close()
            cur.close()

        except (Exception, pg.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()

    def approx_roof_pitch(self):
        conn=None
        try:
            conn = pg.connect(self.db_params)
            cur=conn.cursor()
            cur.execute("SELECT id_lokalid, byg033tagdækningsmateriale FROM buildings")
            building = cur.fetchone() # Retrieve parameters for the first building.
            
            while building is not None:
                bbr_material = building[1] # Retrieve the roof cover material
                if bbr_material in [2,6]: # Depending on the type of roof cover material, the roof pitch is estimated
                    pitch = 10
                elif bbr_material in [3, 5, 10]:
                    pitch = 40
                elif bbr_material in [4, 90]:
                    pitch = 35
                elif bbr_material ==7:
                    pitch = 20
                else:             
                    pitch = 1
                    
                cur2=conn.cursor()
                # Insert the roof pitch value in the "buildings" table for the corresponding building.
                cur2.execute("UPDATE buildings SET roof_pitch = %s WHERE id_lokalid = %s", (pitch, building[0]))
                
                building = cur.fetchone() # Retrieve the next building and iterate.

            conn.commit()
            cur2.close()
            cur.close()

        except (Exception, pg.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()

    def get_roof_structure(self, elems, cyear, pitch): #This function selects all roof structure types that can be used with the building's roof pitch, then selects a random one based on construction year.
        id_list = []
        for elem in elems:
            if elem[4] <= pitch and pitch <= elem[5]:
                id_list.append(elem[0])
        return self.random_possible_element(elems,id_list,cyear)  

    def link_roof_structure(self):
        try:
            conn = pg.connect(self.db_params)
            cur_elem=conn.cursor()
            cur_elem.execute("SELECT * FROM roof_structure_types WHERE name NOT IN ('Ridge board', 'Top floor ceiling') ORDER BY id")
            elems = cur_elem.fetchall() # Retrieve the list of all roof structure types, which can be fed to the random choice function

            cur = conn.cursor()
            cur.execute("DELETE FROM buildings_to_roof_structures")    
            cur.execute("SELECT id_lokalid, byg026opførelsesår, byg021bygningensanvendelse, roof_pitch FROM buildings")
            row = cur.fetchone() # Retrieve information for the first building
                
            cur_write=conn.cursor()
            row_number=0

            while row is not None:
                row_number+=1
                clear_output(wait=True)
                print(row_number)
                pitch=row[-1] # Retrieve the roof pitch
                cyear=row[1] # Retrieve the construction year
                roof_structure=self.get_roof_structure(elems, cyear, pitch) # Select a suitable roof structure based on roof pitch and construction year
                if roof_structure is not None:
                    # Add entry to the mapping table
                    cur_write.execute("INSERT INTO buildings_to_roof_structures(bbr_id, roof_structure_id) VALUES (%s, %s)", (row[0], roof_structure[0]))
                else:
                    # If there is no suitable choice, add NULL to the mapping table.
                    cur_write.execute("INSERT INTO buildings_to_roof_structures(bbr_id) VALUES (%s)", (row[0],))

                row=cur.fetchone() # Retrieve the next building and iterate.

            conn.commit()
            cur_write.close()
            cur_elem.close()
            cur.close()

        except (Exception, pg.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()

    def add_ridge_board(self):
        try:
            conn = pg.connect(self.db_params)
            cur_elem=conn.cursor()
            cur_elem.execute("SELECT id FROM roof_structure_types WHERE name = 'Ridge board'") # Select the ridge board macrocomponent
            elem = cur_elem.fetchone()
            
            cur = conn.cursor()
            cur.execute("""
            SELECT b.id_lokalid
            FROM buildings b 
            INNER JOIN buildings_to_roof_structures btt ON b.id_lokalid=btt.bbr_id
            INNER JOIN roof_structure_types typ ON typ.id=btt.roof_structure_id
            WHERE typ.name NOT LIKE 'Flat%'
            """) # Flat roofs do not have a ridge board, so we get all buildings that don't have a flat roof.
            row = cur.fetchone() # Retrieve information for the first building
            
            cur_write=conn.cursor()
            row_number=0
                
            while row is not None:
                row_number+=1
                clear_output(wait=True)
                print(row_number)
                cur_write.execute("INSERT INTO buildings_to_roof_structures(bbr_id, roof_structure_id) VALUES (%s, %s)", (row[0], elem[0]))

                row=cur.fetchone() # Retrieve the next building and iterate.

            conn.commit()
            cur_elem.close()
            cur_write.close()
            cur.close()

        except (Exception, pg.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()

    def add_top_floor_ceiling(self):
        try:
            conn = pg.connect(self.db_params)
            cur_elem=conn.cursor()
            cur_elem.execute("SELECT id FROM roof_structure_types WHERE name = 'Top floor ceiling'") # Select the top floor ceiling macrocomponent
            elem = cur_elem.fetchone()
            
            cur = conn.cursor()
            cur.execute("""
            SELECT b.id_lokalid
            FROM buildings b 
            INNER JOIN buildings_to_roof_structures btt ON b.id_lokalid=btt.bbr_id
            INNER JOIN roof_structure_types typ ON typ.id=btt.roof_structure_id
            WHERE typ.name NOT IN ('Ridge board', 'Flat wood', 'Flat concrete')
            """) # Flat roofs are assumed not to have additional beams on the top floor ceiling, so we get all buildings that don't have a flat roof.
            row = cur.fetchone() # Retrieve information for the first building
            
            cur_write=conn.cursor()
            row_number=0
                
            while row is not None:
                row_number+=1
                clear_output(wait=True)
                print(row_number)
                cur_write.execute("INSERT INTO buildings_to_roof_structures(bbr_id, roof_structure_id) VALUES (%s, %s)", (row[0], elem[0]))

                row=cur.fetchone() # Retrieve the next building and iterate.

            conn.commit()
            cur_elem.close()
            cur_write.close()
            cur.close()

        except (Exception, pg.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()

    def get_floor(self, elems, cyear): # This function just selects a random appropriate component given the building's construction year
        id_list = []
        for elem in elems:
            id_list.append(elem[0])
        return self.random_possible_element(elems,id_list,cyear)   

    def get_int_wall(self,elems, cyear):
        id_list = []
        for elem in elems:
            id_list.append(elem[0])
        return self.random_possible_element(elems,id_list,cyear)  

    def get_ground_slab(self,elems, cyear):
        id_list = []
        for elem in elems:
            id_list.append(elem[0])
        return self.random_possible_element(elems,id_list,cyear)   

    def get_foundation(self,elems, cyear):
        id_list = []
        for elem in elems:
            id_list.append(elem[0])
        return self.random_possible_element(elems,id_list,cyear)  

    def get_element(self,element,elems,cyear,**kwargs): # Calls the function to select one particular type of element
        var=eval("self.get_"+element)(elems, cyear)
        return var

    def link_other_element(self,element):
        try:
            conn = pg.connect(self.db_params)
            cur_elem=conn.cursor()
            cur_elem.execute("SELECT * FROM %s ORDER BY id" % (element+"_types",))
            elems = cur_elem.fetchall() # Retrieve the list of all possible types for the given element, which can be fed to the random choice function
            
            cur = conn.cursor()
            cur.execute("DELETE FROM buildings_to_"+element+"s")
            cur.execute("SELECT id_lokalid, byg026opførelsesår, byg021bygningensanvendelse FROM buildings")
            row = cur.fetchone() # Get properties from the first building as a tuple

            cur_write=conn.cursor()
            row_number=0

            while row is not None:
                row_number+=1
                clear_output(wait=True)
                print(row_number)
                
                cyear=row[1] # Get the building's construction year
                elem=self.get_element(element,elems, cyear) # Select a suitable type for the given element in this building
                if elem is not None:
                    # Add entry to the mapping table
                    cur_write.execute("INSERT INTO buildings_to_"+element+"s(bbr_id, "+element+"_id) VALUES (%s, %s)", (row[0],elem[0]))
                else:
                    # If there is no suitable choice, add None to the mapping table
                    cur_write.execute("INSERT INTO buildings_to_"+element+"s(bbr_id) VALUES (%s)", (row[0],))

                row=cur.fetchone() # Retrieve the next building as a tuple and iterate

            conn.commit()
            cur_write.close()
            cur_elem.close()
            cur.close()

        except (Exception, pg.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()

    def link_ground_slab(self):
        self.link_other_element('ground_slab')

    def link_int_wall(self):
        self.link_other_element('int_wall')

    def link_foundation(self):
        self.link_other_element('foundation')

    def link_floor(self):
        self.link_other_element('floor')

    def map_building_to_macrocomponents(self,element):
        eval("self.link_"+element)()

    # Functions to map with LCAbyg components and products
    def insert_subcomponent(self, list_of_rows):
        sql = "INSERT INTO subcomponents(lcabyg_id, name, unit, layer, comment) VALUES(%s, %s, %s, %s, %s) ON CONFLICT ON CONSTRAINT subcomponents_pkey DO UPDATE SET (lcabyg_id, name, unit, layer, comment) = (EXCLUDED.lcabyg_id, EXCLUDED.name, EXCLUDED.unit, EXCLUDED.layer, EXCLUDED.comment);"
        connector = None
        bbrid = None
        try:
            # connect to the PostgreSQL database
            connector = pg.connect(self.db_params)
            # create a new cursor
            cur = connector.cursor()
            # execute the INSERT statement
            cur.executemany(sql, list_of_rows)
            # commit the changes to the database
            connector.commit()
            # close communication with the database
            cur.close()
        except (Exception, pg.DatabaseError) as error:
            print(error)
        finally:
            if connector is not None:
                connector.close()

    def add_lcabyg_subcomponents(self, read_lcabyg_constructions):
        list_of_const=[]
        for c in read_lcabyg_constructions:
            for key in c.keys():        
                if key=='Node':
                    ID=c[key]['Construction']['id']
                    name=c[key]['Construction']['name']['English']
                    unit=c[key]['Construction']['unit']
                    layer=c[key]['Construction']['layer']
                    comment=c[key]['Construction']['comment']['Danish']
                    list_of_const.append((ID,name,unit,layer,comment))
        self.insert_subcomponent(list_of_const)

    def insert_product(self,list_of_prods):
        sql = "INSERT INTO products(lcabyg_id, name, comment) VALUES(%s, %s, %s) ON CONFLICT ON CONSTRAINT products_pkey DO UPDATE SET (lcabyg_id, name, comment) = (EXCLUDED.lcabyg_id, EXCLUDED.name, EXCLUDED.comment);"
        connector = None
        bbrid = None
        try:
            # connect to the PostgreSQL database
            connector = pg.connect(self.db_params)
            # create a new cursor
            cur = connector.cursor()
            # execute the INSERT statement
            cur.executemany(sql, list_of_prods)
            # commit the changes to the database
            connector.commit()
            # close communication with the database
            cur.close()
        except (Exception, pg.DatabaseError) as error:
            print(error)
        finally:
            if connector is not None:
                connector.close()

    def add_lcabyg_products(self, read_lcabyg_products):
        list_of_prods=[]
        for c in read_lcabyg_products:
            for key in c.keys():        
                if key=='Node':
                    ID=c[key]['Construction']['id']
                    name=c[key]['Construction']['name']['English']
                    unit=c[key]['Construction']['unit']
                    layer=c[key]['Construction']['layer']
                    comment=c[key]['Construction']['comment']['Danish']
                    list_of_prods.append((ID,name,unit,layer,comment))
        self.insert_product(list_of_prods)

    def map_components_to_products(self,read_lcabyg_constructions):
            self.run_sql("DELETE FROM subcomponents_to_products")

            list_of_edges=[]
            for c in read_lcabyg_constructions:
                for key in c.keys():        
                    if key=='Edge':
                        ID=c[key][0]['ConstructionToProduct']['id']
                        amount=c[key][0]['ConstructionToProduct']['amount']
                        unit=c[key][0]['ConstructionToProduct']['unit']
                        lifespan=c[key][0]['ConstructionToProduct']['lifespan']
                        const_id=c[key][1]
                        prod_id=c[key][2]
                        list_of_edges.append((ID,const_id,prod_id,amount,unit,lifespan))

            sql = "INSERT INTO subcomponents_to_products(id, subcomponent_id, product_id, amount, unit, lifespan) VALUES(%s, %s, %s, %s, %s, %s) ON CONFLICT ON CONSTRAINT subcomponents_to_products_pkey DO UPDATE SET (id, subcomponent_id, product_id, amount, unit, lifespan) = (EXCLUDED.id, EXCLUDED.subcomponent_id, EXCLUDED.product_id, EXCLUDED.amount, EXCLUDED.unit, EXCLUDED.lifespan);"
            connector=None

            try:
                connector = pg.connect(self.db_params)
                
                # fetch construction ids
                cur_const = connector.cursor()
                cur_const.execute('SELECT lcabyg_id FROM subcomponents')
                const_ids=[]
                for i in cur_const.fetchall():
                    const_ids.append(i[0])
                    
                # fetch product ids       
                cur_prod = connector.cursor()
                cur_prod.execute('SELECT lcabyg_id FROM products')
                prod_ids=[]
                for i in cur_prod.fetchall():
                    prod_ids.append(i[0])
                
                # checking for foreign key constraints
                # The LCAbyg edges may include references to products or constructions that are not recorded in the respective files,
                # for instance because a product has been deleted but the corresponding edge hasn't. We need to deal with these cases separately,
                # otherwise the insertion query will raise foreign key constraints (we are trying to insert a row referring to products that do not exist)
                
                valid_edges=[]
                missing_subcomponents=[]
                missing_products=[]
                for edge in list_of_edges:
                    if edge[1] in const_ids and edge[2] in prod_ids:
                        valid_edges.append(edge)
                    if edge[1] not in const_ids:
                        missing_subcomponents.append((edge[1],))
                    if edge[2] not in prod_ids:
                        missing_products.append((edge[2],))
                            
                # insert missing subcomponents
                cur = connector.cursor()
                sql_const = "INSERT INTO subcomponents(lcabyg_id, name) VALUES(%s, 'missing subcomponent') ON CONFLICT ON CONSTRAINT subcomponents_pkey DO NOTHING"
                cur.executemany(sql_const, missing_subcomponents)
                
                # insert missing products
                sql_prod = "INSERT INTO products(lcabyg_id, name) VALUES(%s, 'missing product') ON CONFLICT ON CONSTRAINT products_pkey DO NOTHING"
                cur.executemany(sql_prod, missing_products)
                
                # insert edges in mapping table
                cur.executemany(sql, list_of_edges)
                
                # commit the changes to the database
                connector.commit()
                cur.close()
                cur_const.close()
                cur_prod.close()

            except (Exception, pg.DatabaseError) as error:
                print(error)
            finally:
                if connector is not None:
                    connector.close()

    # Functions to calculate material amounts
    def estimate_lb_internal_walls(self):
        fill_int_walls_lb="""
        WITH iws AS (
        SELECT
        b.id_lokalId AS bbrid,
        intwallsurface
        FROM buildings b,
        LATERAL (SELECT b.byg021BygningensAnvendelse::int4 AS use_code) lt1,
        LATERAL (SELECT COALESCE(b.byg038SamletBygningsareal,b.byg041BebyggetAreal) AS floor_area) lt2,
        LATERAL (SELECT 
                (CASE 
            WHEN use_code IN (110, 120, 121, 122, 130, 131, 132, 325, 510, 520, 521, 522, 523, 529, 530, 540, 585, 590)
                THEN 0.222 * floor_area 
            WHEN use_code IN (140, 150, 160, 185, 190, 320, 321, 322, 324, 329, 390, 410, 411, 412, 413, 414, 415, 419, 531, 532, 533, 534, 539)
                THEN floor_area * (0.4063 + 0.00003489 * floor_area)
            WHEN use_code <@ int4range(420,490)
                THEN floor_area * (0.4063 + 0.00003489 * floor_area)
            WHEN use_code <@ int4range(210,319)
                THEN floor_area*0.1
            WHEN use_code IN (323, 416,535)
                THEN floor_area*0.1
            ELSE NULL
                END) AS intwallsurface) lt
        )

        INSERT INTO buildings (id_lokalId,int_wall_surface_lb)
        SELECT bbrid, intwallsurface FROM iws
        ON CONFLICT ON CONSTRAINT buildings_pkey DO UPDATE SET (id_lokalId,int_wall_surface_lb) = (EXCLUDED.id_lokalId,EXCLUDED.int_wall_surface_lb)"""

        self.run_sql(fill_int_walls_lb)

    def estimate_nlb_internal_walls(self):
        fill_int_walls_nlb="""
        WITH iws AS (
        SELECT
        b.id_lokalId AS bbrid,
        intwallsurface
        FROM buildings b,
        LATERAL (SELECT b.byg021BygningensAnvendelse::int4 AS use_code) lt1,
        LATERAL (SELECT COALESCE(b.byg038SamletBygningsareal,b.byg041BebyggetAreal) AS floor_area) lt2,
        LATERAL (SELECT %s*floor_area AS volume) lt3,
        LATERAL (%s) lt4,
        LATERAL (SELECT (CASE WHEN (b.byg054AntalEtager IS NULL OR b.byg054AntalEtager = 0) THEN b.byg041BebyggetAreal+perimeter*%s ELSE b.byg041BebyggetAreal+perimeter*b.byg054AntalEtager*%s END) AS external_surface) lt5,
        LATERAL (SELECT (CASE WHEN volume<=0 THEN NULL ELSE external_surface/POWER(volume,0.666667) END) AS icomp) lt6,
            LATERAL (SELECT 
                (CASE 
            WHEN use_code IN (110, 120, 121, 122, 130, 131, 132, 325, 510, 520, 521, 522, 523, 529, 530, 540, 585, 590)
                THEN 0.37 * floor_area 
            WHEN use_code IN (140, 150, 160, 185, 190, 320, 321, 322, 324, 329, 390, 410, 411, 412, 413, 414, 415, 419, 531, 532, 533, 534, 539)
                THEN floor_area * (0.1803 + 0.0883 * icomp)
            WHEN use_code <@ int4range(420,490)
                THEN floor_area * (0.1803 + 0.0883 * icomp)
            WHEN use_code <@ int4range(210,319)
                THEN floor_area*0.15
            WHEN use_code IN (323, 416,535)
                THEN floor_area*0.15
            ELSE NULL
                END) AS intwallsurface) lt
        )

        INSERT INTO buildings (id_lokalId,int_wall_surface_nlb)
        SELECT bbrid, intwallsurface FROM iws
        ON CONFLICT ON CONSTRAINT buildings_pkey DO UPDATE SET (id_lokalId,int_wall_surface_nlb) = (EXCLUDED.id_lokalId,EXCLUDED.int_wall_surface_nlb)
        """ % (self.default_floor_height,self.get_perimeter_sql,self.default_floor_height,self.default_floor_height)

        self.run_sql(fill_int_walls_nlb)

    def amounts_ext_walls(self):
        result_ext_wall_sql="""
        WITH quant_table AS
        (SELECT 
            b.id_lokalId bbrid,
            pr.name product,
            amount_product,
            pmap.unit unit
        FROM buildings b
        INNER JOIN buildings_to_ext_walls bmap
            ON b.id_lokalId = bmap.bbr_id
        INNER JOIN ext_wall_types typ
            ON bmap.ext_wall_id=typ.id
        INNER JOIN ext_walls_to_subcomponents submap
            ON typ.id = submap.ext_wall_id
        INNER JOIN subcomponents sc
            ON sc.lcabyg_id = submap.subcomponent_id
        INNER JOIN subcomponents_to_products pmap
            ON pmap.subcomponent_id = sc.lcabyg_id
        INNER JOIN products pr
            ON pr.lcabyg_id = pmap.product_id,
        LATERAL (%s) ltp,
        LATERAL (SELECT (CASE WHEN b.byg054AntalEtager IS NOT NULL THEN perimeter*b.byg054AntalEtager*%s*(1-%s)*pmap.amount ELSE perimeter*%s*(1-%s)*pmap.amount END) AS amount_product) lta)

        INSERT INTO results_material_amounts(element, product, amount, unit, bbr_id)
        SELECT 'ext_wall', product, amount_product, unit, bbrid
        FROM quant_table
        """ % (self.get_perimeter_sql, self.default_floor_height, self.window_wall_ratio, self.default_floor_height, self.window_wall_ratio)

        self.run_sql(result_ext_wall_sql)

    def amounts_windows(self):
        result_window_sql="""
        WITH quant_table AS
        (SELECT 
            b.id_lokalId bbrid,
            pr.name product,
            amount_product,
            pmap.unit unit
        FROM buildings b
        INNER JOIN subcomponents sc ON sc.name = 'Window - iBuildGreen'
        INNER JOIN subcomponents_to_products pmap
            ON pmap.subcomponent_id = sc.lcabyg_id
        INNER JOIN products pr
            ON pr.lcabyg_id = pmap.product_id,
        LATERAL (%s) ltp,
        LATERAL (SELECT (CASE WHEN b.byg054AntalEtager IS NOT NULL THEN perimeter*b.byg054AntalEtager*%s*%s*pmap.amount ELSE perimeter*%s*%s*pmap.amount END) AS amount_product) lta)

        INSERT INTO results_material_amounts(element, product, amount, unit, bbr_id)
        SELECT 'window', product, amount_product, unit, bbrid
        FROM quant_table
        """ % (self.get_perimeter_sql, self.default_floor_height, self.window_wall_ratio, self.default_floor_height, self.window_wall_ratio)

        self.run_sql(result_window_sql)

    def amounts_int_walls(self):
        result_int_wall_sql="""WITH quant_table AS
        (SELECT 
            b.id_lokalId bbrid,
            pr.name product,
            amount_product,
            pmap.unit unit
        FROM buildings b
        INNER JOIN buildings_to_int_walls bmap
            ON b.id_lokalId = bmap.bbr_id
        INNER JOIN int_wall_types typ
            ON bmap.int_wall_id=typ.id
        INNER JOIN int_walls_to_subcomponents submap
            ON typ.id = submap.int_wall_id
        INNER JOIN subcomponents sc
            ON sc.lcabyg_id = submap.subcomponent_id
        INNER JOIN subcomponents_to_products pmap
            ON pmap.subcomponent_id = sc.lcabyg_id
        INNER JOIN products pr
            ON pr.lcabyg_id = pmap.product_id,
        LATERAL (SELECT (b.int_wall_surface_nlb+b.int_wall_surface_lb)*pmap.amount AS amount_product) lta)

        INSERT INTO results_material_amounts(element,product,amount,unit, bbr_id)
        SELECT 'int_wall',product,amount_product, unit, bbrid
        FROM quant_table
        """
        self.run_sql(result_int_wall_sql)

    def amounts_roof_covers(self):
        result_roof_cover_sql="""WITH quant_table AS
        (SELECT 
            b.id_lokalId bbrid,
            pr.name product,
            amount_product,
            pmap.unit unit
        FROM buildings b
        INNER JOIN buildings_to_roof_covers bmap
            ON b.id_lokalId = bmap.bbr_id
        INNER JOIN roof_cover_types typ
            ON bmap.roof_cover_id=typ.id
        INNER JOIN roof_covers_to_subcomponents submap
            ON typ.id = submap.roof_cover_id
        INNER JOIN subcomponents sc
            ON sc.lcabyg_id = submap.subcomponent_id
        INNER JOIN subcomponents_to_products pmap
            ON pmap.subcomponent_id = sc.lcabyg_id
        INNER JOIN products pr
            ON pr.lcabyg_id = pmap.product_id,
        LATERAL (SELECT b.byg041BebyggetAreal/COS(b.roof_pitch*PI()*180) AS roof_surface) lt1,
        LATERAL (SELECT roof_surface*pmap.amount AS amount_product) lt2)

        INSERT INTO results_material_amounts(element,product,amount,unit, bbr_id)
        SELECT 'roof_cover',product,amount_product, unit, bbrid
        FROM quant_table
        """
        self.run_sql(result_roof_cover_sql)

    def amounts_roof_structures(self):
        result_roof_structure_sql="""WITH quant_table AS
        (SELECT 
            b.id_lokalId bbrid,
            pr.name product,
            amount_product,
            pmap.unit unit
        FROM buildings b
        INNER JOIN buildings_to_roof_structures bmap
            ON b.id_lokalId = bmap.bbr_id
        INNER JOIN roof_structure_types typ
            ON bmap.roof_structure_id=typ.id
        INNER JOIN roof_structures_to_subcomponents submap
            ON typ.id = submap.roof_structure_id
        INNER JOIN subcomponents sc
            ON sc.lcabyg_id = submap.subcomponent_id
        INNER JOIN subcomponents_to_products pmap
            ON pmap.subcomponent_id = sc.lcabyg_id
        INNER JOIN products pr
            ON pr.lcabyg_id = pmap.product_id,
        LATERAL (SELECT b.byg041BebyggetAreal/COS(b.roof_pitch*PI()*180) AS roof_surface) lt1,
        LATERAL (SELECT roof_surface*pmap.amount AS amount_product) lt2
        WHERE typ.name NOT IN ('Ridge board', 'Top floor ceiling'))

        INSERT INTO results_material_amounts(element,product,amount,unit, bbr_id)
        SELECT 'roof_structure',product,amount_product, unit, bbrid
        FROM quant_table
        """
        self.run_sql(result_roof_structure_sql)

    def amounts_floors(self):
        result_floor_sql="""WITH quant_table AS
        (SELECT 
            b.id_lokalId bbrid,
            pr.name product,
            amount_product,
            pmap.unit unit
        FROM buildings b
        INNER JOIN buildings_to_floors bmap
            ON b.id_lokalId = bmap.bbr_id
        INNER JOIN floor_types typ
            ON bmap.floor_id=typ.id
        INNER JOIN floors_to_subcomponents submap
            ON typ.id = submap.floor_id
        INNER JOIN subcomponents sc
            ON sc.lcabyg_id = submap.subcomponent_id
        INNER JOIN subcomponents_to_products pmap
            ON pmap.subcomponent_id = sc.lcabyg_id
        INNER JOIN products pr
            ON pr.lcabyg_id = pmap.product_id,
        LATERAL (SELECT (CASE WHEN b.byg054AntalEtager IS NOT NULL THEN b.byg041BebyggetAreal*(b.byg054AntalEtager-1)*pmap.amount ELSE 0 END) AS amount_product) lta)

        INSERT INTO results_material_amounts(element,product,amount,unit, bbr_id)
        SELECT 'floor',product,amount_product, unit, bbrid
        FROM quant_table
        """
        self.run_sql(result_floor_sql)

    def amounts_foundations(self):
        result_foundation_sql="""WITH quant_table AS
        (SELECT 
            b.id_lokalId bbrid,
            pr.name product,
            amount_product,
            pmap.unit unit
        FROM buildings b
        INNER JOIN buildings_to_foundations bmap
            ON b.id_lokalId = bmap.bbr_id
        INNER JOIN foundation_types typ
            ON bmap.foundation_id=typ.id
        INNER JOIN foundations_to_subcomponents submap
            ON typ.id = submap.foundation_id
        INNER JOIN subcomponents sc
            ON sc.lcabyg_id = submap.subcomponent_id
        INNER JOIN subcomponents_to_products pmap
            ON pmap.subcomponent_id = sc.lcabyg_id
        INNER JOIN products pr
            ON pr.lcabyg_id = pmap.product_id,
        LATERAL (SELECT b.byg041BebyggetAreal*pmap.amount AS amount_product) lta)

        INSERT INTO results_material_amounts(element,product,amount,unit, bbr_id)
        SELECT 'foundation',product,amount_product, unit, bbrid
        FROM quant_table
        """
        self.run_sql(result_foundation_sql)

    def amounts_ridge_boards(self):
        result_ridge_board_sql="""WITH quant_table AS
        (SELECT 
            b.id_lokalId bbrid,
            pr.name product,
            amount_product,
            pmap.unit unit
        FROM buildings b
        INNER JOIN buildings_to_roof_structures bmap
            ON b.id_lokalId = bmap.bbr_id
        INNER JOIN roof_structure_types typ
            ON bmap.roof_structure_id=typ.id
        INNER JOIN roof_structures_to_subcomponents submap
            ON typ.id = submap.roof_structure_id
        INNER JOIN subcomponents sc
            ON sc.lcabyg_id = submap.subcomponent_id
        INNER JOIN subcomponents_to_products pmap
            ON pmap.subcomponent_id = sc.lcabyg_id
        INNER JOIN products pr
            ON pr.lcabyg_id = pmap.product_id,
        LATERAL (SELECT SQRT(b.byg041BebyggetAreal)*%s AS beam_length) lt1,
        LATERAL (SELECT beam_length*pmap.amount AS amount_product) lt2
        WHERE typ.name = 'Ridge board')

        INSERT INTO results_material_amounts(element,product,amount,unit, bbr_id)
        SELECT 'ridge_board',product,amount_product, unit, bbrid
        FROM quant_table
        """ % self.space_efficiency
        self.run_sql(result_ridge_board_sql)

    def amounts_ground_slabs(self):
        result_ground_slab_sql="""WITH quant_table AS
        (SELECT 
            b.id_lokalId bbrid,
            pr.name product,
            amount_product,
            pmap.unit unit
        FROM buildings b
        INNER JOIN buildings_to_ground_slabs bmap
            ON b.id_lokalId = bmap.bbr_id
        INNER JOIN ground_slab_types typ
            ON bmap.ground_slab_id=typ.id
        INNER JOIN ground_slabs_to_subcomponents submap
            ON typ.id = submap.ground_slab_id
        INNER JOIN subcomponents sc
            ON sc.lcabyg_id = submap.subcomponent_id
        INNER JOIN subcomponents_to_products pmap
            ON pmap.subcomponent_id = sc.lcabyg_id
        INNER JOIN products pr
            ON pr.lcabyg_id = pmap.product_id,
        LATERAL (SELECT b.byg041BebyggetAreal*pmap.amount AS amount_product) lta)

        INSERT INTO results_material_amounts(element,product,amount,unit, bbr_id)
        SELECT 'ground_slab',product,amount_product, unit, bbrid
        FROM quant_table
        """
        self.run_sql(result_ground_slab_sql)

    def calculate_material_amounts(self):
        self.run_sql("DELETE FROM results_material_amounts")
        self.amounts_ext_walls()
        self.amounts_windows()
        self.amounts_int_walls()
        self.amounts_floors()
        self.amounts_foundations()
        self.amounts_ground_slabs()
        self.amounts_ridge_boards()
        self.amounts_roof_covers()
        self.amounts_roof_structures()

















