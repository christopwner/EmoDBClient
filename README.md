# EmoDBClient

Sample Java client of EmoDB using Solr for indexing

Created docker image for use with this example.
 
Run docker using:<br>
>docker run --name emodb -d -p 8080:8080 -p 8081:8081 christopwner/emodb

Also relies on Solr. Run with docker:
>docker run --name solr -d -p 8983:8983 -t solr

>docker exec -it --user=solr solr bin/solr create_core -c emodb

Will also require addition configuration of Solr for setting our own version constraints on update by adding the following to the **UpdateRequestProcessorChain**:

~~~
<processor class="solr.DocBasedVersionConstraintsProcessorFactory">
  <str name="versionField">my_version_l</str>
</processor>
~~~

Will need to apt update container and install text editor of choice but config can be accessed by:
>docker exec -it --user=solr solr /bin/nano server/solr/emodb/conf/solrconfig.xml
