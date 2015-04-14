# GeoMesa Tools

## Introduction
GeoMesa Tools is a set of command line tools to add feature management functions, query planning and explanation, ingest, and export abilities from 
the command line.  

## Configuration
To begin using the command line tools, first build the full GeoMesa project from the GeoMesa source directory with 

    mvn clean install
    
You can also make the build process significantly faster by adding `-DskipTests`. This will create a file "geomesa-bin.tar.gz" 
in the geomesa-assemble/target directory. Untar this file with

    tar xvfz geomesa-assemble/target/geomesa-${version}-bin.tar.gz
    
Next, `cd` into the newly created directory with
    
    cd geomesa-${version}

GeoMesa Tools relies on a GEOMESA_HOME environment variable. Running
    
    . bin/geomesa configure

with the `. ` prefix will set this for you, add $GEOMESA_HOME/bin to your PATH, and source your new environment variables in your current shell session.

Now, you should be able to use GeoMesa from any directory on your computer. To test, `cd` to a different directory and run:

    geomesa

This should print out the following usage text: 

    bash$ geomesa
    Usage: geomesa [command] [command options]
      Commands:
        create       Create a feature definition in a GeoMesa catalog
        delete       Delete a feature's data and definition from a GeoMesa catalog
        describe     Describe the attributes of a given feature in GeoMesa
        explain      Explain how a GeoMesa query will be executed
        export       Export a GeoMesa feature
        help         Show help
        ingest       Ingest a file of various formats into GeoMesa
        list         List GeoMesa features for a given catalog
        tableconf    Perform table configuration operations
                
This usage text lists the available commands. To see help for an individual command run `geomesa help <command-name>` which for example
will give you something like this:

    bash$ geomesa help list
    List GeoMesa features for a given catalog
    Usage: list [options]
      Options:
        -a, --auths
           Accumulo authorizations
      * -c, --catalog
           Catalog table name for GeoMesa
        -i, --instance
           Accumulo instance name
        -mc, --mock
           Run everything with a mock accumulo instance instead of a real one
           Default: false
        -p, --password
           Accumulo password (will prompt if not supplied)
      * -u, --user
           Accumulo user name
        -v, --visibilities
           Accumulo scan visibilities
        -z, --zookeepers
           Zookeepers (host[:port], comma separated)

The Accumulo username and password is required for each command. Specify the username and password
in each command by using `-u` or `--username `and `-p` or `--password`, respectively. One can also only specify the username
on the command line using `-u` or `--username` and type the password in an additional prompt, where the password will be
hidden from the shell history.

A test script is included under `geomesa\bin\test-geomesa` that runs each command provided by geomesa-tools. Edit this script
by including your Accumulo username, password, test catalog table, test feature name, and test SFT specification. Default values
are already included in the script. Then, run the script from the command line to ensure there are no errors in the output text. 

In all commands below, one can add `--instance-name`, `--zookeepers`, `--auths`, and `--visibilities` (or in short form `-i, -z, -a, -v`) arguments
to properly configure the Accumulo data store connector. The Accumulo instance name and Zookeepers string can usually
be automatically assigned as long as Accumulo is configured correctly. The Auths and Visibilities strings will have to
be added as arguments to each command, if needed.

###Enabling Shape File Support
Due to licensing restrictions, a necessary dependency (jai-core) for shape file support must be manually installed:
    
    <dependency>
      <groupId>javax.media</groupId>
      <artifactId>jai_core</artifactId>
      <version>1.1.3</version>
    </dependency>
    
This library can be downloaded from your local nexus repo or `http://download.java.net/media/jai/builds/release/1_1_3/jai-1_1_3-lib.zip`

To install, copy the jai_core.jar and jai_code.jar into `$GEOMESA_HOME/lib/`

Optionally there is a script bundled as `$GEOMESA_HOME/bin/install-jai` that will attempt to wget and install 
the jai libraries

###Logging configuration
GeoMesa tools comes bundled by default with an slf4j implementation that is installed to the $GEOMESA_HOME/lib directory
 named `slf4j-log4j12-1.7.5.jar` If you already have an slf4j implementation installed on your Java Classpath you may
 see errors at runtime and will have to exclude one of the jars. This can be done by simply renaming the bundled
 `slf4j-log4j12-1.7.5.jar` file to `slf4j-log4j12-1.7.5.jar.exclude`
 
Note that if no slf4j implementation is installed you will see this error:

    SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder".
    SLF4J: Defaulting to no-operation (NOP) logger implementation
    SLF4J: See http://www.slf4j.org/codes.html#StaticLoggerBinder for further details.

## Command Explanations and Usage

### create
To create a new feature on a specified catalog table, use the `create` command.  

#### Usage (required options denoted with star):
    $ geomesa help create
    Create a feature definition in a GeoMesa catalog
    Usage: create [options]
      Options:
        -a, --auths
           Accumulo authorizations
      * -c, --catalog
           Catalog table name for GeoMesa
        -dt, --dtg
           DateTime field name to use as the default dtg
      * -fn, --feature-name
           Simple Feature Type name on which to operate
        -i, --instance
           Accumulo instance name
        -mc, --mock
           Run everything with a mock accumulo instance instead of a real one
           Default: false
        -p, --password
           Accumulo password (will prompt if not supplied)
        -sh, --shards
           Number of shards to use for the storage tables (defaults to number of
           tservers)
      * -s, --spec
           SimpleFeatureType specification
        -st, --use-shared-tables
           Use shared tables in Accumulo for feature storage (true/false)
           Default: true
      * -u, --user
           Accumulo user name
        -v, --visibilities
           Accumulo scan visibilities
        -z, --zookeepers
           Zookeepers (host[:port], comma separated)

#### Example command:
    geomesa create -u username -p password -c test_create -i instname -z zoo1,zoo2,zoo3 -fn testing -s fid:String:index=true,dtg:Date,geom:Point:srid=4326 -dt dtg

### removeschema
To remove a feature type and it's associated data from a catalog table, use the `removeschema` command.

####Usage (required options denoted with star):
    $ geomesa help removeschema
    Usage: removeschema [options]
      Options:
        -a, --auths
           Accumulo authorizations
      * -c, --catalog
           Catalog table name for GeoMesa
        -fn, --feature-name
           Simple Feature Type name on which to operate
        -f, --force
           Force deletion without prompt
           Default: false
        -i, --instance
           Accumulo instance name
        -mc, --mock
           Run everything with a mock accumulo instance instead of a real one
           (true/false)
           Default: false
        -p, --password
           Accumulo password (will prompt if not supplied)
        -pt, --pattern
           Regular expression to select items to delete
      * -u, --user
           Accumulo user name
        -v, --visibilities
           Accumulo scan visibilities
        -z, --zookeepers
           Zookeepers (host[:port], comma separated)


    
####Example:
    geomesa removeschema -u username -p password -i instname -z zoo1,zoo2,zoo3 -c test_catalog -fn testfeature1
    geomesa removeschema -u username -p password -i instname -z zoo1,zoo2,zoo3 -c test_catalog -pt 'testfeatures\d+'

### deletecatalog
To delete a GeoMesa catalog completely (and all features in it) use the `deletecatalog` command.

####Usage (required options denoted with star):
    geomesa help deletecatalog
    Delete a GeoMesa catalog completely (and all features in it)
    Usage: deletecatalog [options]
      Options:
        -a, --auths
           Accumulo authorizations
      * -c, --catalog
           Catalog table name for GeoMesa
        -f, --force
           Force deletion without prompt
           Default: false
        -i, --instance
           Accumulo instance name
        -mc, --mock
           Run everything with a mock accumulo instance instead of a real one
           (true/false)
           Default: false
        -p, --password
           Accumulo password (will prompt if not supplied)
      * -u, --user
           Accumulo user name
        -v, --visibilities
           Accumulo scan visibilities
        -z, --zookeepers
           Zookeepers (host[:port], comma separated)
    
####Example:
    geomesa deletecatalog -u username -p password -i instname -z zoo1,zoo2,zoo3 -c test_catalog
    
### describe
To describe the attributes of a feature on a specified catalog table, use the `describe` command.  

####Usage (required options denoted with star):
    $ geomesa help describe
    Describe the attributes of a given feature in GeoMesa
    Usage: describe [options]
      Options:
        -a, --auths
           Accumulo authorizations
      * -c, --catalog
           Catalog table name for GeoMesa
      * -fn, --feature-name
           Simple Feature Type name on which to operate
        -i, --instance
           Accumulo instance name
        -mc, --mock
           Run everything with a mock accumulo instance instead of a real one (true/false)
           Default: false
        -p, --password
           Accumulo password (will prompt if not supplied)
      * -u, --user
           Accumulo user name
        -v, --visibilities
           Accumulo scan visibilities
        -z, --zookeepers
           Zookeepers (host[:port], comma separated)

      
#### Example command:
    geomesa describe -u username -p password -c test_delete -fn testing
 
### explain
To ask GeoMesa how it intends to satisfy a given query, use the `explain` command.

####Usage (required options denoted with star):
    $ geomesa help explain
    Explain how a GeoMesa query will be executed
    Usage: explain [options]
      Options:
        -a, --auths
           Accumulo authorizations
      * -c, --catalog
           Catalog table name for GeoMesa
      * -q, --cql
           CQL predicate
      * -fn, --feature-name
           Simple Feature Type name on which to operate
        -i, --instance
           Accumulo instance name
        -mc, --mock
           Run everything with a mock accumulo instance instead of a real one
           Default: false
        -p, --password
           Accumulo password (will prompt if not supplied)
      * -u, --user
           Accumulo user name
        -v, --visibilities
           Accumulo scan visibilities
        -z, --zookeepers
           Zookeepers (host[:port], comma separated)


#### Example command:
    geomesa explain -u username -p password -c test_catalog -fn test_feature -q "INTERSECTS(geom, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28)))"

### export
To export features, use the `export` command.  

####Usage (required options denoted with star):
    $ geomesa help export
    Export a GeoMesa feature
    Usage: export [options]
      Options:
        -at, --attributes
           Attributes from feature to export (comma-separated)...Comma-separated
           expressions with each in the format attribute[=filter_function_expression]|derived-attribute=filter_function_expression.
           filter_function_expression is an expression of filter function applied to attributes, literals and
           filter functions, i.e. can be nested
        -a, --auths
           Accumulo authorizations
      * -c, --catalog
           Catalog table name for GeoMesa
        -q, --cql
           CQL predicate
        -dt, --dt-attribute
           name of the date attribute to export
      * -fn, --feature-name
           Simple Feature Type name on which to operate
        -fmt, --format
           Format to export (csv|tsv|gml|json|shp|bin)
           Default: csv
        -id, --id-attribute
           name of the id attribute to export
        -i, --instance
           Accumulo instance name
        -lat, --lat-attribute
           name of the latitude attribute to export
        -lon, --lon-attribute
           name of the longitude attribute to export
        -lbl, --label-attribute
           name of the label attribute to export
        -max, --max-features
           Maximum number of features to return. default: Long.MaxValue
           Default: 2147483647
        -mc, --mock
           Run everything with a mock accumulo instance instead of a real one
           Default: false
        -o, --output
           name of the file to output to instead of std out
        -p, --password
           Accumulo password (will prompt if not supplied)
      * -u, --user
           Accumulo user name
        -v, --visibilities
           Accumulo scan visibilities
        -z, --zookeepers
           Zookeepers (host[:port], comma separated)

<b>attribute expressions</b>
Attribute expressions are comma-separated expressions with each in the format 
    
    attribute[=filter_function_expression]|derived-attribute=filter_function_expression. 
    
`filter_function_expression` is an expression of filter function applied to attributes, literals and filter functions, i.e. can be nested

#### Example commands:
    geomesa export -u username -p password -c test_catalog -fn test_feature -at "geom,text,user_name" -fmt csv -q "include" -m 100
    geomesa export -u username -p password -c test_catalog -fn test_feature -at "geom,text,user_name" -fmt gml -q "user_name='JohnSmith'"
    geomesa export -u username -p password -c test_catalog -fn test_feature -at "user_name,buf=buffer(geom\, 2)" \
        -fmt csv -q "[[ user_name like `John%' ] AND [ bbox(geom, 22.1371589, 44.386463, 40.228581, 52.379581, 'EPSG:4326') ]]"
    
### ingest
Ingests CSV, TSV, and SHP files from the local file system and HDFS. CSV and TSV files can be ingested either with explicit latitude and longitude columns or with a column of WKT geometries.
For lat/lon column ingest, the sft spec must include an additional geometry attribute in the sft beyond the number of columns in the file such as: `*geom:Point`.
The file type is inferred from the extension of the file, so ensure that the formatting of the file matches the extension of the file and that the extension is present.
*Note* the header if present is not parsed by Ingest for information, it is assumed that all lines are valid entries.

####Usage (required options denoted with star):
    $ geomesa help ingest
    Ingest a file of various formats into GeoMesa
    Usage: ingest [options] <file>...
      Options:
        -a, --auths
           Accumulo authorizations
      * -c, --catalog
           Catalog table name for GeoMesa
        -cols, --columns
           the set of column indexes to be ingested, must match the
           SimpleFeatureType spec (zero-indexed)
        -dtf, --dt-format
           format string for the date time field
        -dt, --dtg
           DateTime field name to use as the default dtg
      * -fn, --feature-name
           Simple Feature Type name on which to operate
        -fmt, --format
           format of incoming data (csv | tsv | shp) to override file extension
           recognition
        -h, --hash
           flag to toggle using md5hash as the feature id
           Default: false
        -id, --id-fields
           the set of attributes to combine together to create a unique id for the
           feature (comma separated)
        -is, --index-schema
           GeoMesa index schema format string
        -i, --instance
           Accumulo instance name
        -lat, --lat-attribute
           name of the latitude field in the SimpleFeature if latitude is 
           kept in the SFT spec; otherwise defines the csv field index used to create the
           default geometry
        -lon, --lon-attribute
           name of the longitude field in the SimpleFeature if longitude is 
           kept in the SFT spec; otherwise defines the csv field index used to create the
           default geometry
        -mc, --mock
           Run everything with a mock accumulo instance instead of a real one
           Default: false
        -p, --password
           Accumulo password (will prompt if not supplied)
        -sh, --shards
           Number of shards to use for the storage tables (defaults to number of
           tservers)
      * -s, --spec
           SimpleFeatureType specification
        -st, --use-shared-tables
           Use shared tables in Accumulo for feature storage (true/false)
           Default: true
      * -u, --user
           Accumulo user name
        -v, --visibilities
           Accumulo scan visibilities
        -z, --zookeepers
           Zookeepers (host[:port], comma separated)


#### Example commands:

##### Ingest CSV with single WKT (Well Known Text) geometry

    # file.csv (comma separated)
    featureId,date,geom
    1.23623623,01/01/2014 06:30:23,POINT(2 3)
    290.43234,01/02/2014 08:35:24,POINT(3 3)
    3.14159,01/03/2014 18:11:56,POINT(4 5)

    # ingest command
    geomesa ingest -u username -p password -c geomesa_catalog -fn myfeature -s 'fid:Double,dtg:Date,*geom:Geometry' \
        --dtg dtg -dtf "MM/dd/yyyy HH:mm:ss" hdfs:///some/hdfs/path/to/file.csv

##### Ingest lon/lat that are part of the SimpleFeatureType using a feature id made of the hash of two fields
 
    # file.tsv (tab separated)
    2.3623  01/01/2014 06:30:23 38.023 -74.0002
    23.333  01/01/2014 06:30:23 39.424 -76.02
    10.021  01/01/2014 06:30:23 40.325 -75.883
    
    # ingest command
    geomesa ingest -u username -p password -c geomesa_catalog -a someAuths -v someVis -sh 42 -fn myfeature
     -s fid:Double,dtg:Date,lon:Double,lat:Double,*geom:Point -dt dtg -dtf "MM/dd/yyyy HH:mm:ss" 
     -id fid,dtg -h -lon lon -lat lat /some/local/path/to/file.tsv

##### Create geometry field from the lon/lat but drop the lon/lat from the FeatureType spec - List[Int] is ingested

    # file.csv (comma separated
    "2014-01-01 22:33:44","38.023","-74.0002","5,6,7"
    "2014-01-02 22:33:44","39.023","-78.0002",""
    "2014-01-03 22:33:44","50.023","-73.0002","1, 2, 3"
    
    # ingest command
    geomesa ingest -u username -p password -c geomesa_catalog -fn myfeature
     -s 'dtg:Date,myList:List[Int],*geom:Point' -cols '0,3' -dt dtg -dtf "MM-dd-yyyy HH:mm:ss" 
     -lon 1 -lat 2 /some/local/path/to/file.csv

##### Create geometry field from the lon/lat but drop the lon/lat from the FeatureType spec - Map[String,Int] is ingested

    # file.csv (comma separated
    "2014-01-01 22:33:44","38.023","-74.0002","a,5;b,6;c,7"
    "2014-01-02 22:33:44","39.023","-78.0002",""
    "2014-01-03 22:33:44","50.023","-73.0002","a,1;b,2;d,3"

    # ingest command
    geomesa ingest -u username -p password -c geomesa_catalog -fn myfeature
     -s 'dtg:Date,myMap:Map[String,Int],*geom:Point' -cols '0,3' -dt dtg -dtf "MM-dd-yyyy HH:mm:ss"
     -lon 1 -lat 2 -md "," ";" /some/local/path/to/file.csv

##### Ingest a shape file
    geomesa ingest -u username -p password -c test_catalog -f shapeFileFeatureName /some/path/to/file.shp

### list
To list the features on a specified catalog table, use the `list` command.  

####Usage (required options denoted with star):
    $ geomesa help list
    List GeoMesa features for a given catalog
    Usage: list [options]
      Options:
        -a, --auths
           Accumulo authorizations
      * -c, --catalog
           Catalog table name for GeoMesa
        -i, --instance
           Accumulo instance name
        -mc, --mock
           Run everything with a mock accumulo instance instead of a real one
           Default: false
        -p, --password
           Accumulo password (will prompt if not supplied)
      * -u, --user
           Accumulo user name
        -v, --visibilities
           Accumulo scan visibilities
        -z, --zookeepers
           Zookeepers (host[:port], comma separated)


#### Example command:
    geomesa list -u username -p password -c test_catalog
    
### tableconf
To list, describe, and update the configuration parameters on a specified table, use the `tableconf` command. 

####Usage (required options denoted with star):
    $ geomesa help tableconf
    Perform table configuration operations
    Usage: tableconf [options] [command] [command options]
      Commands:
        list      List the configuration parameters for a geomesa table
          Usage: list [options]
            Options:
              -a, --auths
                 Accumulo authorizations
            * -c, --catalog
                 Catalog table name for GeoMesa
            * -fn, --feature-name
                 Simple Feature Type name on which to operate
              -i, --instance
                 Accumulo instance name
              -mc, --mock
                 Run everything with a mock accumulo instance instead of a real one
                 Default: false
              -p, --password
                 Accumulo password (will prompt if not supplied)
            * -t, --table-suffix
                 Table suffix to operate on (attr_idx, st_idx, or records)
            * -u, --user
                 Accumulo user name
              -v, --visibilities
                 Accumulo scan visibilities
              -z, --zookeepers
                 Zookeepers (host[:port], comma separated)
    
        describe      Describe a given configuration parameter for a table
          Usage: describe [options]
            Options:
              -a, --auths
                 Accumulo authorizations
            * -c, --catalog
                 Catalog table name for GeoMesa
            * -fn, --feature-name
                 Simple Feature Type name on which to operate
              -i, --instance
                 Accumulo instance name
              -mc, --mock
                 Run everything with a mock accumulo instance instead of a real one
                 Default: false
            *     --param
                 Accumulo table configuration param name (e.g. table.bloom.enabled)
              -p, --password
                 Accumulo password (will prompt if not supplied)
            * -t, --table-suffix
                 Table suffix to operate on (attr_idx, st_idx, or records)
            * -u, --user
                 Accumulo user name
              -v, --visibilities
                 Accumulo scan visibilities
              -z, --zookeepers
                 Zookeepers (host[:port], comma separated)
    
        update      Update a given table configuration parameter
          Usage: update [options]
            Options:
              -a, --auths
                 Accumulo authorizations
            * -c, --catalog
                 Catalog table name for GeoMesa
            * -fn, --feature-name
                 Simple Feature Type name on which to operate
              -i, --instance
                 Accumulo instance name
              -mc, --mock
                 Run everything with a mock accumulo instance instead of a real one
                 Default: false
            * -n, --new-value
                 New value of the property
            *     --param
                 Accumulo table configuration param name (e.g. table.bloom.enabled)
              -p, --password
                 Accumulo password (will prompt if not supplied)
            * -t, --table-suffix
                 Table suffix to operate on (attr_idx, st_idx, or records)
            * -u, --user
                 Accumulo user name
              -v, --visibilities
                 Accumulo scan visibilities
              -z, --zookeepers
                 Zookeepers (host[:port], comma separated)


#### Example commands:
    geomesa tableconf list -u username -p password -c test_catalog -fn test_feature -t st_idx
    geomesa tableconf describe -u username -p password -c test_catalog -fn test_feature -t attr_idx --param table.bloom.enabled
    geomesa tableconf update -u username -p password -c test_catalog -fn test_feature -t records --param table.bloom.enabled -n true

### repl
To drop into an interactive REPL, use the `repl` command. The REPL is the scala console with GeoMesa-specific
functionality. In addition to normal GeoMesa usage, the scalding REPL is included. More details on the
features exposed by scalding can be read here: [Scalding-REPL](https://github.com/twitter/scalding/wiki/Scalding-REPL)

####Usage:
    $ geomesa repl
    # Launches the REPL. Enter commands just as you would at the scala repl.
    $ geomesa repl hdfs
    # Launches the REPL in distributed mode, where any jobs will be run on your hadoop cluster. Requires
    # a local hadoop installation.

#### Example command:
    geomesa repl