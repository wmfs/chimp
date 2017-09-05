# Chimp

> An [award-winning](https://github.com/wmfs/chimp/wiki/Chimp-in-the-news), dynamic PostgreSQL database generator written in Python... with integrated extract, transform and load capabilities.

# Environment

There are quite a few things you'll need up-front to give your Chimp the best possible start, but they're all easily installed for free (on either Windows or Linux) and you should be done within an hour or two... 

## Python
Chimp is written in the Python scripting language, so you'll need that if you haven't already. 
More specifically, you'll need **Python 3.2**. **[You can download Python here](http://www.python.org/download/)**. 

**Important!** You'll need to have Python accessible from any directory
* In a Windows environment you'll need to ensure your PATH variable includes the path where you can find `python.exe`, for example: `C:\Program Files\Python32`
* It's worth testing things at this point... on the command line, from any directory, type "python". You should then see something similar to that below:

**Windows**

```
Python 3.2 (r32:88445, Feb 20 2011, 21:29:02) [MSC v.1500 32 bit (Intel)] on win32
Type "help", "copyright", "credits" or "license" for more information.
>>>
```

**Linux**

```
Python 3.2.2 (default, Mar  6 2012, 17:58:42)
[GCC 4.4.6 20110731 (Red Hat 4.4.6-3)] on linux2
Type "help", "copyright", "credits" or "license" for more information.
>>>
```

* If this doesn't work, check your PATH variable.
* To exit Python type `exit()`.

## PostgreSQL
You'll also need a Chimp-friendly database. Chimp will only behave itself in a **PostgreSQL 9.x** database. There are no plans to encourage Chimp to play nice with any other database environments (or older versions of PostgreSQL).
**[You can download the latest PostgreSQL here](http://www.postgresql.org/download/)**.

## PsycoPG2
Chimp uses **PsycoPG2** to communicate with PostgreSQL. **[The main download page for PsycoPG2 is available here](http://pypi.python.org/pypi/psycopg2/)**. If Chimp is to be used on a Windows operating system, then the [installers available here](http://www.stickpeople.com/projects/python/win-psycopg/) may prove useful.
* It's important to install a version of PsycoPG2 that's happy with both Python 3.2 and the version of PostgreSQL Chimp will be using.
   
## PostGIS
Chimp is remarkably adept at solving spatial problems. Once your PostgreSQL database is up-and-running, install PostGIS over the top of it. PostGIS extends PostgreSQL with tools which Chimp can use to help produce content-rich maps and spatially-enabled searches. **[You can download the latest PostGIS here](http://postgis.refractions.net/download/)**.
* Make sure you install a version of PostGIS that's compatible with your version of PostgreSQL.

## Create a database
You'll now need to create a blank PostgreSQL database for Chimp to use. 

The bundled **PGAdmin III** tool makes this process straightforward, **[there's a video about doing it here](http://www.youtube.com/watch?v=1wvDVBjNDys)**.
Some notes:
* The recommended name for your first database is "**chimp**" (all lowercase). This will reduce the amount of configuration required and keep the information in this Wiki more relevant.
* Chimp will need to be able to connect to your database as a superuser. The default "postgres" user is fine to start with.
* Be sure to create your new database using the **PostGIS template** (possibly called "template_postgis"). You'll know if this has worked because once your database is created there should be well over 700 functions in your database's "public" schema.

Alternatively, you may prefer to create a database via **psql**, with something similar to:
* `CREATE DATABASE chimp WITH OWNER = postgres TEMPLATE = template_postgis ENCODING = 'UTF8';`

## Solr

All search duties within Chimp are handed-over to the popular and amazingly fast **Apache Solr** search engine. Chimp can generate, rank and publish documents to an **Apache Solr 3.6** server, **[which is available to download here](http://lucene.apache.org/solr/)**.
* Chimp also supports Solr 3.5. 
* Chimp does not _yet_ support Solr 4.0 (which is currently available only as an alpha release).

## GDAL
To make the most of Chimp's spatial skills you'll also need to install **GDAL** (Geospatial Data Abstraction Library). GDAL provides tools for working with spatial data, which Chimp can use out-of-the-box. To do some really cool stuff (such as loading and styling complex mapping data) Chimp really needs **GDAL version 1.9** or greater. **[You can download the latest GDAL 1.9 here](http://trac.osgeo.org/gdal/wiki/DownloadingGdalBinaries)**. If Chimp is to be used on a Windows operating system, then the [installers available here](http://www.gisinternals.com/sdk/) may be of use.

# Installation and configuration

Getting your new Chimp installed and operational is as easy as extracting a .zip file, writing a small script/batch file and changing a few lines of configuration. However, it's important to have prepared a suitable environment for your Chimp before going any further.

## Installation
The quickest way to get yourself up-and-running with **Chimp** is to either download and extract a **.zip** file or a **.gz** file, using one of the links below:

* **[Download a Chimp as a .zip file](https://github.com/wmfs/chimp/zipball/master)**

* **[Download a Chimp as a .gz file](https://github.com/wmfs/chimp/tarball/master)**

Alternatively, Chimp is available as a Git repository on GitHub: https://github.com/wmfs/chimp/. Hooking into a GitHub repository is best if you plan to take frequent updates of Chimp.

## Configuration

### Chimp execution

Once you've grabbed/extracted Chimp you'll need to locate your **Chimp root directory**. It contains `README.md` and (amongst others) the following directories:
* `chimp` Contains the Python (.py) files that Chimp requires to do its thing.
* `config` Contains a variety of XML files that configure Chimp to your environment. 
* `resources` Contains pre-built scripts and directory structures that Chimp will refer to when it starts building things.

As part of setting up your Chimp environment, you'll already be able to run Python from any directory via the command line. Ideally, you'll also want to be able to use Chimp from any directory as well. There are many ways to achieve this, and things are pretty different on Windows and Linux. Below are some suggestions...

#### Windows

* On the root of Chimp you'll find a `scripts` directory.
* Create a new `chimp.bat` file in the `scripts` directory.
* Edit this file so it contains the following:

```
@echo off
pushd "%~dp0"
cd ..
python .\src\chimp.py %*
popd
echo on
```

* Now alter the Windows PATH variable to include the ``scripts`` directory.
* Restart your command line.
* You'll now be able to access Chimp by typing ``chimp`` from any directory.
* Test this by typing ``chimp --help``, you should see Chimp's help page.

#### Linux

* Assuming you have decided to hold your latest copy of chimp in your eclipse workspace
* and you have perhaps done a "git clone https://github.com/wmfs/chimp/"
* Then the following commands will allow you to get it running

```
alias python3='/opt/python3/bin/python3'
alias chimp='python3 /home/tim/workspace/wmfs/chimp/src/chimp.py'
```

* Note you cannot use chimp until you have configured the next section as it will throw an error

### settings.xml

In Chimp's `config` directory you'll find a file called `settings.xml.default`.
* First, **copy** `settings.xml.default` to a new file called `settings.xml` (this new file should also be located in the `config` directory).
* Now edit your **new** `settings.xml` file.
* You'll see Chimp's settings have a section called **registry**.
There are a few values in the **registry** section you will need to change:

#### `repositoryPath`

* Chimp will use a **repository** to store information about your data-sets and also write any files it subsequently generates. Set this value to somewhere _outside_ your chimp directory. Some notes:
 * Change `repositoryPath` to point to a new directory that will be the root of your not-yet-built **repository**
 * This path should be **away** from your existing "chimp" directory
 * The final directory in this path should **not** exist already (Chimp will make it for you when it builds your repository)

#### ``psqlExecutable``
* Set this value to where the ```psql``` executable can be found. This is a command-line tool that is installed as part of PostgreSQL. Chimp uses psql to automatically install specifications into your PostgreSQL database. 
* In a Windows environment, the default location of **psql.exe** is ```C:\Program Files\PostgreSQL\9.0\bin\psql.exe```
* In a Linux environment it will likely be ```/usr/bin/psql```

#### ``srid``
* This is the Spatial Reference System Identifier (SRID) indicating which coordinate system Chimp should work with.
* Some popular coordinate systems:
 * `4326`  World Geodetic System - AKA Lat/Long  
 * `27700` British National Grid
 * `29901` Irish National Grid
* For a list of SRIDs Chimp supports, connect to a PostGIS-enabled datbase and run the following SQL:

```    
select *
from public.spatial_ref_sys
order by srid
```

## Creating database connections
Next you'll need to tell Chimp how to get to the database you created while setting-up its [[environment|Environment]].

* In Chimp's `config` directory you'll see a `connections` directory
* Similar to the settings.xml technique above, copy the `chimp.xml.default` to a new file in the same directory called `chimp.xml`.
* Now edit the **new** `chimp.xml` file
* You'll see it contains entries similar to that shown below:

```xml
<connection 
  label="Local Chimped-database" 
  host="localhost" 
  vendor="PostgreSQL" 
  version="9.0"
  dbname="chimp"
  user="postgres"
  password="postgres"
  port="5432"
/>
```

* These attributes reflect PostgreSQL defaults. Alter the various attributes in `chimp.xml` to reflect your PostgreSQL 9.x database you've already created.
* Take care to set `dbname` to the name you provided while setting up your Chimp environment.
* In this instance "chimp" is the name of a **database connection** that Chimp can use.
* You can have as many _database connections_ as you like, just create a new file in the `connections` (again copy `chimp.xml.default` to a new name such as `sandpit.xml` or `production.xml` and edit the attributes accordingly)

### Installing Chimp
Now it's time to install the database objects Chimp will need into your PostgreSQL database. From the command line, run the following:

```
chimp install --dbconnection chimp
```

This will generate all objects into the database identified by the "chimp" **database connection**.

### Building a Chimp repository

As discussed earlier, Chimp uses a **repository** to store information about your data-sets and also write any files it subsequently generates. Please ensure ``repositoryPath`` is set in ``settings.xml`` as per the previous instructions, before running the following command:

    chimp create --entitytype repository


### Defining zones

Chimp has an idea of concentric geographical **zones** in which your data can be located.

![Zone diagram](http://wmfs.github.com/chimp/zone_diagram.png)

* The most inner (perhaps most _local_) zone is known as  **Zone 1**
* Zone 2 then envelops Zone 1, and so on (as per the diagram above)
* Chimp can use this information in a variety of ways, for example to restrict search results depending on what zone a record falls.
* Zones are also defined in `settings.xml`, as shown below:

```
<zones>
  <zone id="2" schema="areas" table="my_organisation_boundary_plus_10k_buffer" column="wkb_geometry"/>
  <zone id="1" schema="areas" table="my_organisation_boundary" column="wkb_geometry"/>
</zones>
```

* As you'll see, each zone is mapped to a database table (as identified by the `schema` and `table` attributes).
* Chimp provides an `areas` schema for you to put tables like this in.
* These tables should have a single row that contain the boundary of the zone in a PostGIS geometry column (identified by the ``column`` attribute).
* The order of things here is important, and somewhat counter-intuitive as you'll note zones appear in **descending order**. This is to help things run a bit quicker as Chimp will perform spatial tests in the order they appear in ``settings.xml``. If a test fails (i.e. a point does not fall inside the associated polygon) then no further tests are performed. For example, if a point is not within Zone 2, then it follows it cannot be inside Zone 1 either, so that test need not be performed.
* If you have boundaries in a common GIS format such as **.shp** or **.tab** then you can use **GDAL** to quickly import it:

```
ogr2ogr -f "PostgreSQL" PG:"host=localhost user=postgres dbname=chimp password=postgres" "my_boundary_file.shp" -preserve_fid  -t_srs EPSG:27700 -s_srs EPSG:27700 -nln areas.my_boundary
```

* Note that Chimp has the idea of a **default zone**. The default zone `id` is defined in the `registry` section of `settings.xml`. If a point does not fall inside any zone-related polygon it will instead receive the id that is defined here. Consider the diagram above, no Zone 3 is defined explicitly, this is instead the _default zone_.

```
<key name="defaultZoneId" value="3" />
```

With your zones set-up in `settings.xml` and the supporting tables loaded into your PostgreSQL database (typically in the ```areas``` schema), the final step is to register your zones with Chimp. The following command will get this done:

```
chimp install --zones --dbconnection chimp
```

## Configuring Solr

Now attention shifts to getting a **Solr** search engine integrated with Chimp. This section assumes you've a Solr server up-and-running already (if this isn't the case then please see the tutorials available [[here|http://lucene.apache.org/solr]]).

In Chimp's `config/solr` directory you'll find two files:

* `solr-fields.xml.default`
* `solr-settings.xml.default`

Just like the `settings.xml` technique used previously, copy both these files into the same directory,  minus the ``.default`` suffix, so that you end up with two new files (`solr-fields.xml` and `solr-settings.xml`). You won't need to alter either of these files at this stage.

When your repository was installed, Chimp will have created a `default` **Solr server**  configuration for you. First, locate your Chimp repository directory. From there, you'll find a file located:

```
solr_servers/default/solr_server.xml
```

**Edit** this file and you'll see a `solr` tag like that shown below:

```
<solr version="3.6" url="http://localhost:8983/solr" connection="chimp"/>
```

You'll need to change the attributes in this tag to integrate Chimp with your Solr server:

* `version` Set this to either **3.5** or **3.6**, depending on your Solr server version.
* `url` This is the where your Solr server can be found.
* `connection` This is the name of the **database connection** which Chimp will make searchable via your Solr server.

With that in order, the final step is to build all the necessary database objects to support your Solr server. The following command will do this:

```
chimp build --solrserver default --install --dbconnection chimp
```

## Configuring External Loaders

When things get a little more specialised, Chimp can defer staging duties to **External Loaders**. For example, Chimp uses the Geospatial Data Abstraction Library **(GDAL)** to integrate with a wealth of GIS formats. All external loaders are found in Chimp's `config/external_loaders` directory. The example below shows how to configure GDAL for use by Chimp...

As usual, in Chimp's `config/external_loaders/gdal` directory, copy the `loader_config.xml.default` file to a new ``loader_config.xml`` file (again in the same directory). Then **edit** the new `loader_config.xml` file, you'll see something similar to that below:

```
 <loaderConfig>
     <registry>
         <key name="currentWorkingDirectory" value="C:/Program Files/GDAL/gdal-data"/>
         <key name="commandName" value="C:/Program Files/GDAL/ogr2ogr"/>
     </registry>
 </loaderConfig>
```

Alter the `currentWorkingDirectory` value to the path where GDAL's `gdal-data` directory can be found, then change the `commandName` value to the location of GDAL's `ogr2ogr` command.

* Repeat this technique for any other External Loaders you wish to configure (they're all to be found in Chimp's `config/external_loaders` directory).

# License

[GPL V3](https://github.com/wmfs/chimp/blob/master/LICENSE)




