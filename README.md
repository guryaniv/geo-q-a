# Geographical Q&A

In [geo_qa.py](https://github.com/guryaniv/geo-q-a/blob/master/geo_qa.py) I implemented a web crawler, that extracts information about all existing countries in the world from [Wikipedia](https://en.wikipedia.org/wiki/List_of_countries_by_population_(United_Nations)).

Using XPATH, we extract the following input from each country's wiki page (with Multi-Threading):
* Population
* Capital
* Area
* Government 
* President
* Prime Minister
* For each Prime minister or President, we also extract the date of birth.

The mentioned info is stored as relations in an ontology, created using rdflib.
You can see the ontology itself in [ontology.nt](https://github.com/guryaniv/geo-q-a/blob/master/ontology.nt).

The ontology can be queried using SPARQL.
In addition you can query the ontology by natural language, using the following question structures:
1. Who is the president of $country?
2. Who is the prime minister of $country?
3. What is the population of $country?
4. What is the area of $country?
5. What is the government of $country?
6. What is the capital of $country?
7. When was the president of $country born?
8. When was the prime minister of $country born?
9. Who is $entity?

<em>Note: $country may refer to any country in the world, $entity refers to a certain president or prime minister</em>

[geo_ontology_queries.py](https://github.com/guryaniv/geo-q-a/blob/master/geo_ontology_queries.py) containes some SPARQL queries that answer specific questions about the ontology, [geo_queries_results.txt](https://github.com/guryaniv/geo-q-a/blob/master/geo_queries_results.txt) contains the answers.

**Run Instructions**:<br>
To create the ontology, run from the cmd line:<br>
```python geo_qa.py create ontology.nt```<br>
To query an existing ontology, run:<br>
```python geo_qa.py question $your natural language question string$``` 
