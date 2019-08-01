import rdflib


def query_a(g, f):
    f.write("\n*****a*****\n")
    qres = g.query(
        """select DISTINCT (COUNT(?pm) AS ?p)
           where {  
           ?c <http://en.wikipedia.org/wiki/prime_minister> ?pm .  
           }""")
    for row in qres:
        f.write("There are %s prime ministers in the world.\n" % row)


def query_b(g, f):
    f.write("\n*****b*****\n")
    qres = g.query(
        """select DISTINCT (COUNT(?country) AS ?c)
           where {  
           ?country <http://en.wikipedia.org/wiki/type> <http://en.wikipedia.org/wiki/country> .
           }""")
    for row in qres:
        f.write("There are %s countries in the world.\n" % row)


def query_c(g, f):
    f.write("\n*****c*****\n")
    qres = g.query(
        """select (COUNT (?government) AS ?g) 
           where {  
           ?country <http://en.wikipedia.org/wiki/government> ?government . 
           FILTER (contains(lcase(str(?government)), "republic")) 
           }""")
    for row in qres:
        f.write("%s countries in the world are republics.\n" % row)


def query_d(g, f):
    f.write("\n*****d*****\n")
    qres = g.query(
        """select (COUNT (?government) AS ?g) 
           where {  
           ?country <http://en.wikipedia.org/wiki/government> ?government . 
           FILTER (contains(lcase(str(?government)), "monarchy")) 
           }""")
    for row in qres:
        f.write("%s countries in the world are monarchies.\n" % row)


if __name__== "__main__":
    f = open('geo_queries_results.txt', 'w')
    g = rdflib.Graph()
    g.parse("ontology.nt", format="nt") # our ontology from previous section
    query_a(g, f)
    query_b(g, f)
    query_c(g, f)
    query_d(g, f)
