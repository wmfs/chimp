'''
Created on 12 Jan 2012

@author: Ryan Pickett
'''
from psql import PSQLExecutor
import os
from tempfile import mkstemp
from shutil import copyfileobj

SHARED_SCHEMA = "shared"


def installChimp(settings, zone):
#    (fd, path) = mkstemp()
    
    # concatenate install/*.sql files into temporary file and execute
#    file = os.fdopen(fd, "w")
    if not zone:
        installScriptsDir = os.path.join(settings.paths["resources"], "install")
        filenames = [f for f in os.listdir(installScriptsDir) if not f.startswith(".") and f.lower().endswith(".sql")]
        filenames.sort()
        for filename in filenames:
            PSQLExecutor(settings).execute(os.path.join(installScriptsDir, filename))
    else:
        # ====================
        # DROP ZONE OBJECTS
        # ====================        
        dropZoneFilename = os.path.join(settings.paths["repository"], "scripts", "generated", "zone files","drop_zones.sql")
        if os.path.exists(dropZoneFilename):
            PSQLExecutor(settings).execute(dropZoneFilename)         
        file = open(dropZoneFilename, "w")                        
        file.write("-- Drop zone objects\n")                        
        for zone in settings.zones:
            for dataType in ["double precision", "numeric"]:
                file.write("DROP FUNCTION IF EXISTS {0}.is_point_within_{1}({2}, {2});\n".format(SHARED_SCHEMA, zone.table, dataType))
        file.close()

        # ====================
        # INSTALL ZONE OBJECTS
        # ====================        
        installZoneFilename = os.path.join(settings.paths["repository"], "scripts", "generated", "zone files","install_zones.sql")
        file = open(installZoneFilename, "w")                        
        script = "-- Install zone objects:\n\n"
    
        for zone in settings.zones:
            for dataType in ["double precision", "numeric"]:
                srid = int(settings.env["srid"])
                script += ("CREATE OR REPLACE FUNCTION {5}.is_point_within_{1}(p_x {4}, p_y {4})\n" 
                           "  RETURNS boolean AS $$\n"
                           "DECLARE\n"
                           "  v_within boolean;\n"
                           "BEGIN\n"
                           "  SELECT ST_Within(ST_GeomFromText('POINT('||p_x||' '||p_y||')',{3}),{2})\n" 
                           "  INTO v_within\n"
                           "  FROM {0}.{1};\n" 
                           "  RETURN v_within;\n"
                           "END;\n"
                           "$$ LANGUAGE plpgsql STRICT IMMUTABLE;\n\n\n".format(zone.schema, zone.table, zone.column, srid, dataType, SHARED_SCHEMA))        

        for dataType in ["double precision", "numeric"]:
            logic = ""
            c=0
            indent=""
            for zone in settings.zones:
                if c>0:
                    logic += "  IF v_zone IS NOT NULL THEN\n"                
                logic += ("  {3}IF {0}.is_point_within_{1}(p_x, p_y) THEN\n"
                          "    {3}v_zone={2};\n"
                          "  {3}END IF;\n").format(SHARED_SCHEMA, zone.table, zone.id,indent)
                if c>0:
                    logic += "  END IF;\n"                
                c+=1
                indent="  "
                
            script += ("CREATE OR REPLACE FUNCTION {0}.get_zone(p_x {1}, p_y {1})\n" 
                       "  RETURNS integer AS $$\n"
                       "DECLARE\n"
                       "  v_zone integer;\n"
                       "BEGIN\n{2}"                       
                       "  IF v_zone IS NULL THEN\n"
                       "    v_zone = {3};\n"
                       "  END IF;\n"
                       "  RETURN v_zone;\n"
                       "END;\n"
                       "$$ LANGUAGE plpgsql STRICT IMMUTABLE;\n\n\n".format(SHARED_SCHEMA, dataType, logic, settings.env["defaultZoneId"]))        
        
        
        
        file.write(script)          
        file.close()
        
        PSQLExecutor(settings).execute(installZoneFilename)
    
#    file.close()
#    os.remove(path)