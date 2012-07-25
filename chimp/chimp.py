# http://diveintopython3.org/xml.html

import chimpinstall
import chimpbuild
import chimpclean
import chimpcreate
import extract
import settings as chimpSettings
from taskqueue.Queue import Queue
from taskqueue.StreamProcessor import StreamProcessor
from load.Loader import Loader
from taskqueue.Queuer import Queuer
import calc.solr as solr
import chimptools
settings = chimpSettings.Settings()

command = settings.args.command




if command == "install":
    chimpinstall.installChimp(settings, settings.args.zones)
    
elif command == "build":
    install = settings.args.reinstall or settings.args.install
    drop = settings.args.reinstall or settings.args.drop      
    
    if settings.args.specification is not None:        
        chimpbuild.buildSpecificationScripts(settings, install, drop)

    if settings.args.solrserver is not None:
        chimpbuild.buildSolrServerScripts(settings, settings.args.solrserver, install,drop)
    
    
    
elif command == "import":    
    supportConnection = settings.db.makeConnection("support")
    supportCursor = supportConnection.makeCursor("supportCursor", False, False)
    
    queue = Queue(supportConnection, supportCursor, settings)
    queuer = Queuer(supportConnection, supportCursor, settings, queue)
    queuer.queueImport(settings.args.groupid)

    if settings.args.postimportcompute in("specification", "full"):
        if settings.args.postimportcompute=="specification":
            restriction = settings.specification.name
        else:
            restriction = None 
        queuer.queueCalculation(None, restriction, settings.args.streamname, settings.args.groupid)
        
        
#    supportConnection = settings.db.makeConnection("support")
#    supportCursor = supportConnection.makeCursor("supportCursor", False, False)
#    queue = Queue(supportConnection, supportCursor, settings)
#    queuer = Queuer(supportConnection, supportCursor, settings, queue)
#    queuer.queueCalculation(settings.args.groupid)
       
#    if not settings.args.deferprocessing:
#        loader = Loader(supportConnection, supportCursor, settings, queue)
#        StreamProcessor(supportConnection, supportCursor, settings, queue, loader).processStream(False)        
#    queue.close()

    
    if not settings.args.deferprocessing:
        loader = Loader(supportConnection, supportCursor, settings, queue)
        StreamProcessor(supportConnection, supportCursor, settings, queue, loader).processStream(False)
        
    queue.close()
    
elif command == "queue":
    supportConnection = settings.db.makeConnection("support")
    supportCursor = supportConnection.makeCursor("supportCursor", False, False)
    
    queue = Queue(supportConnection, supportCursor, settings)
    
    if settings.args.action=="clear":
        queue.clear()
    elif settings.args.action=="restart":
        loader = Loader(supportConnection, supportCursor, settings, queue)
        StreamProcessor(supportConnection, supportCursor, settings, queue, loader).processStream(True)
    elif settings.args.action=="stop":
        queue.stop(settings.args.streamname)
    queue.close()

elif command == "clean":
    chimpclean.clean(settings, force=settings.args.force);

elif command == "create":
    if settings.args.entitytype == "specification":
        chimpcreate.createSpecification(settings, settings.args.name)    
    else:
        chimpcreate.createSolrServer(settings, settings.args.name)

elif command=="extract":
    extractProcessor = extract.Extract(settings)
    extractProcessor.debug(settings.appLogger)

elif command=="tool":
    toolProcessor = chimptools.runTool(settings)



if command=="compute":
    supportConnection = settings.db.makeConnection("support")
    supportCursor = supportConnection.makeCursor("supportCursor", False, False)
    queue = Queue(supportConnection, supportCursor, settings)
    queuer = Queuer(supportConnection, supportCursor, settings, queue)
    queuer.queueCalculation(settings.args.restriction, settings.args.specificationrestriction, settings.args.streamname, settings.args.groupid)
       
    if not settings.args.deferprocessing:
        loader = Loader(supportConnection, supportCursor, settings, queue)
        StreamProcessor(supportConnection, supportCursor, settings, queue, loader).processStream(False)        
    queue.close()

if command == "solr":
    solrServer = solr.SolrServer(settings.paths["config"], settings.args.server)
    solrServer.debug(settings.appLogger) 
    solrServer.export(settings.appLogger)
    
print("Done.")
