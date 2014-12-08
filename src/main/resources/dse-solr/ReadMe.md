This will be a repository illustrating how to use dse-solr, this will illustrate how to index `movies_genre` collection
inserted by `benchmark` application.

Once you have inserted data into cassandra using benchmark:

Create a Solr core:

    curl http://127.0.0.1:8983/solr/resource/moviedata.movies_genre/solrconfig.xml --data-binary @solrconfig.xml -H 'Content-type:text/xml; charset=utf-8'

    curl http://127.0.0.1:8983/solr/resource/moviedata.movies_genre/schema.xml --data-binary @schema.xml -H 'Content-type:text/xml; charset=utf-8'

    curl "http://127.0.0.1:8983/solr/admin/cores?action=CREATE&name=moviedata.movies_genre"

Now, you can search cassandra using Solr HTTP API, ex: search for all movie names that have "The" in them:

    curl 'http://127.0.0.1:8983/solr/moviedata.movies_genre/select?q=movie_name%3A*The*&wt=json&indent=on&omitHeader=on'