import rdflib, sys, re, requests
from rdflib import Literal, XSD, URIRef, Graph
import lxml.html
import threading
import time
import urllib

wiki_prefix1 = "http://en.wikipedia.org"
wiki_prefix2 = "http://en.wikipedia.org/wiki/"
ontology = Graph()
countries_url = "https://en.wikipedia.org/wiki/List_of_countries_by_population_(United_Nations)"   # table of countries
testing = False
one_to_six_flag = False
seven_and_eight_flag = False

answers_to_questions = {
    1: "Sergio Mattarella",
    2: "Theresa May",
    3: "91,931,000",
    4: "18,274 km2",
    5: "Unitary parliamentary absolute diarchy",
    6: "Ottawa",
    7: "1953-01-24",
    8: "1980-07-26",
    9: "President of American Samoa, Guam, Northern Mariana Islands, United States, United States Virgin Islands",
    10: "Prime minister of Greece",
    11: "37,314,442",
    12: "San Jose",
    13: "1954-10-24",
    14: "440.0 km2",
    15: "Unitary presidential Islamic republic",
    16: "Andrej Plenković",
    17: "20,770–22,072 km2",
    18: "Prime minister of Israel",
    19: "Prime minister of Faroe Islands",
}


def main():
    if len(sys.argv) < 2:
        print("Error: not enough arguments")
        return
    if sys.argv[1] == "create":
        t0 = time.time()
        get_countries()
        ontology.serialize("ontology.nt", format="nt")
        t1 = time.time()
        print("Finished creating ontology in:", t1 - t0, "seconds")
        return
    elif sys.argv[1] != "question":
        print("Error: wrong arguments")
        return
    else:
        g = rdflib.Graph()
        g.parse("ontology.nt", format="nt")  # our ontology that was previously created
        if testing:
            i = 1
            test(g, i, "Who is the president of Italy?")
            i += 1
            test(g, i, "Who is the prime minister of United Kingdom?")
            i += 1
            test(g, i, "What is the population of Democratic Republic of the Congo?")
            i += 1
            test(g, i, "What is the area of Fiji?")
            i += 1
            test(g, i, "What is the government of Eswatini (Swaziland)?")
            i += 1
            test(g, i, "What is the capital of Canada?")
            i += 1
            test(g, i, "When was the president of South Korea born?")
            i += 1
            test(g, i, "When was the prime minister of New Zealand born?")
            i += 1
            test(g, i, "Who is Donald Trump?")
            i += 1
            test(g, i, "Who is Alexis Tsipras?")
            i += 1
            test(g, i, "What is the population of Canada?")
            i += 1
            test(g, i, "What is the capital of Costa Rica?")
            i += 1
            test(g, i, "When was the president of Vanuatu born?")
            i += 1
            test(g, i, "What is the area of Antigua_and_Barbuda?")
            i += 1
            test(g, i, "What is the government of Afghanistan?")
            i += 1
            test(g, i, "Who is the prime minister of Croatia?")
            i += 1
            test(g, i, "What is the area of Israel?")
            i += 1
            test(g, i, "Who is Benjamin Netanyahu?")
            i += 1
            test(g, i, "Who is Aksel V. Johannesen?")
            i += 1
        else:
            entity, relation = parse_question(sys.argv[2])
            last_case_flag = False
            if relation is not '':
                query = "<{wiki_prefix2}{entity}> <{wiki_prefix2}{relation}>".format(wiki_prefix2=wiki_prefix2,
                                                                                     entity=entity,
                                                                                     relation=relation)
            else:
                query = "<{wiki_prefix2}{entity}>".format(wiki_prefix2=wiki_prefix2, entity=entity)
            if 'When' in sys.argv[2]:
                qres = query_cases_sevend_and_eight(g, query)
            elif relation is '':  # case 9
                qres = query_last_case(g, query)
                last_case_flag = True
            else:  # one_to_six for now
                qres = query_cases_one_to_six(g, query)
            res, relation = extract_res(qres, last_case_flag)
            if 'capital' in sys.argv[2]:    # to handle cases like san jose, costa rica
                if len(res.split(',')) > 1:
                    if res.split(',')[1] != ' D.C.':
                        res = res.split(',')[0]
            if 'area' in sys.argv[2]:
                res = res + ' km2'
            elif last_case_flag:
                if '_' in relation:  # prime_minister
                    relation = "Prime minister"
                else:  # president
                    relation = relation.title()
                res = "{} of {}".format(relation, res)
            print(res)


# *** CREATE FUNCTIONS *** #

def add_to_ontology(entity1, relation, entity2, is_term=False):
    """
    adds ?entity1 <relation> ?entity2 to the ontology graph
    to be called by all other functions getting info
    """
    ent1 = URIRef(entity1)
    rel = URIRef(wiki_prefix2+relation.replace(" ", "_").lower())
    if is_term:
        ent2 = entity2
    else:
        ent2 = URIRef(entity2)
    ont_lock.acquire()  # we use a lock to add to the ontology to allow multi-threading
    ontology.add((ent1, rel, ent2))
    ont_lock.release()


def get_dob(entity):
    """
    adds to the ontology:
    ?entity <birth_date> ?dob
    :param entity: url to the entity's (pm or president) page
    """
    http_lock.acquire()
    res = requests.get(entity)
    http_lock.release()
    doc = lxml.html.fromstring(res.content)
    dob = doc.xpath("//table[contains(@class, 'infobox')]//th[contains(text(),'Born')]")
    if not len(dob):
        dob = doc.xpath("//table[contains(@class, 'infobox')]//th[contains(text(),'Date of birth')]")
    if not len(dob):
        return  # no dob found
    else:
        dob = dob[0].xpath("./../td//span[@class='bday']//text()")
        if len(dob):
            dob = dob[0]
            add_to_ontology(entity, "birth_date", Literal(dob, datatype=XSD.date), True)
        else:
            dob = doc.xpath("//table[contains(@class, 'infobox')]//th[contains(text(),'Born')]/../td/text()")
            if len(dob):
                if len(dob[0]) >= 4:    # at least a year (otherwise not a date)
                    dob = dob[0]
                    dob = dob.replace('/', ' ')     # for assumed year we just take the first assumption
                    dob_tokens = dob.split()
                    dob = dob_tokens[0]
                    if len(dob) < 4:
                        if dob == "c.":
                            dob = dob_tokens[1]
                        else:
                            dob = dob_tokens[2]
                    add_to_ontology(entity, "birth_date", Literal(dob, datatype=XSD.date), True)


def get_country_info(country):
    """"
    add to the ontology:
    ?country <population> ?population
    ?country <capital> ?capital
    ?country <area> ?area
    ?country <government> ?government
    ?country <president> ?president
    ?country <prime_minister> ?pm
    :param country: url to country's page
    """
    http_lock.acquire()
    res = requests.get(country)
    http_lock.release()
    doc = lxml.html.fromstring(res.content)
    infobox = doc.xpath("//table[contains(@class, 'infobox')]")
    if len(infobox):
        infobox = infobox[0]
    else:
        return  # no infobox to extract info from

    # ?country <population> ?population
    pop_row = infobox.xpath(".//th/a[contains(text(), 'Population')]/../../following-sibling::tr")  # get the next row after population header
    if not len(pop_row):
        pop_row = infobox.xpath(".//tr/th[contains(text(), 'Population')]/../following-sibling::tr")
    if len(pop_row):
        pop_row = pop_row[0]
        if len(pop_row.xpath(".//td//text()")):
            pop = pop_row.xpath(".//td//text()")[0]   # get population value
            if len(pop):
                if pop[0] == " ":
                    pop = pop[1:]
                pop = pop.split()[0]
                add_to_ontology(country, "population", Literal(pop, datatype=XSD.integer), True)

    # ?country <capital> ?capital
    capital_lst = infobox.xpath(".//th[contains(text(), 'Capital')]/../td/a/@href")
    prefix2_flag = 0  # for capitals that we got via text and not link, need to add a different prefix
    if len(capital_lst) == 0:
        capital_lst = infobox.xpath(".//th[contains(text(), 'Capital')]/../td//text()")
        prefix2_flag = 1
    if len(capital_lst) != 0:
        if capital_lst[0] == "\n" or capital_lst[0] == ' ':
            capital = capital_lst[1]
        else:
            capital = capital_lst[0]
        if capital[-1] == "(":
            capital = capital[:-2]
        if prefix2_flag:
            add_to_ontology(country, "capital", wiki_prefix2+capital.replace(" ", "_").lower())
        else:
            add_to_ontology(country, "capital", wiki_prefix1+capital)

    # ?country <area> ?area
    area_row = infobox.xpath(".//th/a[contains(text(), 'Area')]/../../following-sibling::tr")    # get the next row after area header
    if len(area_row):
        area_row = area_row[0]
        if len(area_row.xpath(".//th/div[contains(text(), 'Total')]")):     # check that it's the total area row
            area = area_row.xpath(".//th/div[contains(text(), 'Total')]/../following-sibling::td/text()")   # get area value
            if len(area):
                area = area[0].split()[0]
                area = re.sub("[^0-9.,/–-]", " ", area)
                area = area.split()[0]
                add_to_ontology(country, "area", Literal(area, datatype=XSD.float), True)

    # ?country <government> ?government
    gov_lst = infobox.xpath(".//th//a[contains(text(), 'Government')]/../../td//text()")
    if not len(gov_lst):
        gov_lst = infobox.xpath(".//th[contains(text(), 'Government')]/../td//text()")
    if len(gov_lst):
        gov = ""
        if len(gov_lst) == 1:
            gov = gov_lst[0].replace(" ", "_")
        else:
            for gov_element in gov_lst:
                if gov_element != " " and gov_element != "\xa0" and gov_element != ":" \
                        and gov_element.isdigit() is not True and gov_element[0] != "[":
                    if gov != "":
                        if gov[-1] == " ":
                            gov = gov + gov_element
                        else:
                            gov = gov + " " + gov_element
                    else:
                        gov = gov + gov_element
        if gov != "":
            gov = re.sub("[^a-zA-Z_()-]", " ", gov)
            gov = gov.replace(" u ", "_").replace(" ", "_").replace("__", "_").replace("__", "_")
            gov = gov.split("_de_facto")[0]
            gov = gov.split("_(_de_jure_)")[0]
            gov = gov.split("_(de_jure)")[0]
            if len(gov.split("de_jure")) > 1:
                gov = gov.split("de_jure")[1]
            if gov[0] == "_" or gov[0] == " ":
                gov = gov[1:]
            add_to_ontology(country, "government", wiki_prefix2+gov)

    # ?country <president> ?president and/or ?country <prime_minister> ?pm
    president = infobox.xpath(".//th//a[.//text()='President']/../../../td//a/@href")
    pm = infobox.xpath(".//th//a[contains(text(), 'Prime Minister')]/../../../td//a/@href")
    if not len(president):
        president = infobox.xpath(".//td//a[.//text()='President']/../../..//a/@href")
    if not len(president):
        president = infobox.xpath(".//th//a[.//text()=' President']/../../../td//a/@href")
    if len(president):
        president = wiki_prefix1+president[0]
        add_to_ontology(country, "president", president)
        get_dob(president)
    if not len(pm):
        pm = infobox.xpath(".//th//a[contains(text(), 'Prime Minister')]/../../../../td//a/@href")
    if not len(pm):
        pm = infobox.xpath(".//th//div[contains(text(), 'Prime Minister')]/../../td//a/@href")
    if len(pm):
        pm = wiki_prefix1+pm[0]
        add_to_ontology(country, "prime_minister", pm)
        get_dob(pm)


def worker(country_url):
    """
    thread worker function for getting all necessary country info
    """
    get_country_info(country_url)


def get_countries():
    """
    adds to the ontology:
    ?country <type> country
    and calls get_country_info using multi-threading to add other relations
    """
    res = requests.get(countries_url)   # get page with countries table
    doc = lxml.html.fromstring(res.content)
    countries_rows = doc.xpath("//table//th[contains(text(), 'Rank')]/../../tr")    # all table rows
    threads = []    # for multi-threading
    for tr in range(2, len(countries_rows)):    # first line is world, iterate over all others
        country = countries_rows[tr].xpath("./td[2]//a/@href")[0]  # /wiki/countryname
        country_url = wiki_prefix1 + country
        if len(countries_rows[tr].xpath("./td[2]//a/@href")) > 2:
            country2 = countries_rows[tr].xpath("./td[2]//a/@href")[2]
            country2_url = wiki_prefix1 + country2
            add_to_ontology(country2_url, "type", wiki_prefix2 + "country")
            thread = threading.Thread(name=str(tr), target=worker, args=(country2_url,))
            threads.append(thread)
            thread.start()
        # population = countries_rows[tr].xpath("./td[5]/text()")[0]  # population in 2016
        # if population[-1] == "\n":
        #     population = population[:-1]
        # add_to_ontology(country_url, "population", Literal(population, datatype=XSD.integer), True)
        add_to_ontology(country_url, "type", wiki_prefix2+"country")
        thread = threading.Thread(name=str(tr), target=worker, args=(country_url,))
        threads.append(thread)
        thread.start()
    for t in threads:
        t.join()


# *** QUESTION FUNCTIONS *** #

def parse_question(line):
    """"
    parses the given question
    :param line: question line from user, from the 9 structures described
    :return: entity (person or country) and relation (prime_minister/population/area/...) as described
    """
    sentence = line.split()
    first_word = sentence[0].lower()
    if first_word == "when":  # cases number 7,8
        seven_and_eight_flag = True
        the_index_first = sentence.index("the")
        of_index_first = sentence.index("of")
        relation = "_".join(sentence[the_index_first+1: of_index_first])
        relation = relation.replace(" ", "_")
        born_index_first = sentence.index("born?")
        entity = "_".join(sentence[of_index_first+1: born_index_first])
        entity = entity.replace(" ", "_")
    elif first_word == "who":
        only_entity_flag = False
        try:
            the_index_first = sentence.index("the")
            of_index_first = sentence.index("of")
        except:
            only_entity_flag = True
        if only_entity_flag:  # case number 9
            entity = "_".join(sentence[2:])
            entity = entity[:-1].replace(" ", "_")
            relation = ""
        else:  # cases number 1,2
            one_to_six_flag = True
            entity = "_".join(sentence[of_index_first+1:])  # got the No handlers could be found for logger "rdflib.term"
            entity= entity[:-1]
            relation = "_".join(sentence[the_index_first +1 : of_index_first]).replace(" ", "_")
    elif first_word == "what":  # cases 3,4,5,6
        one_to_six_flag = True
        the_index_first = sentence.index("the")
        of_index_first = sentence.index("of")
        relation = sentence[3]
        entity = "_".join(sentence[of_index_first+1:])
        entity = entity[:-1].replace(" ", "_")
    if "_(" in entity:
        entity = entity.split('_')
        entity = entity[0]
    return entity, relation


def query_cases_one_to_six(g, query):
    qres = g.query("select ?t where {\n" + query + " ?t .\n}")
    return qres


def query_cases_sevend_and_eight(g, query):
    qres = g.query(
        """select ?p 
           where {""" +
           """{query} ?bp .  
           ?bp <http://en.wikipedia.org/wiki/birth_date> ?p .""".format(query=query) + """ 
           }""")
    return qres


def query_last_case(g, query):
    qres = g.query(
        """select ?rel ?country 
           where{ """ +
           """?rel ?country {query} .""".format(query=query) + """
           }""")
    return qres


def extract_res(qres, last_case_flag):
    answers = []
    relation = None
    for row in qres:
        try:
            res = row[0].split(wiki_prefix2, 1)[1]
            if last_case_flag:
                relation = row[1].split(wiki_prefix2, 1)[1]
        except:
            res = row[0]
        res = res.replace("_", " ")
        res = urllib.parse.unquote(res)
        answers.append(res)
    res = ', '.join(sorted(answers))
    return res, relation


def test(g, test_number, question):
    entity, relation = parse_question(question)
    last_case_flag = False
    query = "<{wiki_prefix2}{entity}> <{wiki_prefix2}{relation}>".format(wiki_prefix2=wiki_prefix2, entity=entity,
                                                                          relation=relation)
    if 'When' in question:
        qres = query_cases_sevend_and_eight(g, query)
    elif relation is '':  # case 9
        query = "<{wiki_prefix2}{entity}>".format(wiki_prefix2=wiki_prefix2, entity=entity)
        last_case_flag = True
        qres = query_last_case(g, query)
    else:  # one_to_six for now
        qres = query_cases_one_to_six(g, query)
    res, relation = extract_res(qres,last_case_flag)
    if 'area' in question:
        res = res + ' km2'
    elif last_case_flag:
        if '_' in relation:  # prime_minister
            relation = "Prime minister"
        else:  # president
            relation = relation.title()
        res = "{} of {}".format(relation,res)
    if res == answers_to_questions[test_number]:
        print("##Passed test number {}##".format(test_number))
    else:
        print("Failed at test number {}, entity: {entity}, relation: {relation} and res: {res}".format(test_number, entity=entity, relation=relation,res=res))


if __name__ == "__main__":
    ont_lock = threading.Lock()     # lock for writing to the ontology
    http_lock = threading.Lock()    # lock for requesting html
    main()
