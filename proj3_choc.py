import sqlite3
import csv
import json
import sqlite3 as sqlite

# proj3_choc.py
# You can change anything in this file you want as long as you pass the tests
# and meet the project requirements! You will need to implement several new
# functions.

# Part 1: Read data from CSV and JSON into a new database called choc.db
DBNAME = 'choc.db'
BARSCSV = 'flavors_of_cacao_cleaned.csv'
COUNTRIESJSON = 'countries.json'


def create_choc_db():
    # Your code goes here
    conn = sqlite.connect(DBNAME)
    cur = conn.cursor()

    statement = '''
        DROP TABLE IF EXISTS 'Bars';
    '''
    cur.execute(statement)

    statement = '''
        DROP TABLE IF EXISTS 'Countries';
    '''
    cur.execute(statement)

    if conn:
        pass
    else:
        print("Failed to create database")

    conn.commit()
    conn.close()


def populate_choc_db():

    conn = sqlite.connect(DBNAME)
    cur = conn.cursor()

    statement = '''
    
    CREATE Table 'Bars'(
    'ID' INTEGER PRIMARY KEY AUTOINCREMENT,
    'Company' TEXT NOT NULL,
    'SpecificBeanBarName' TEXT NOT NULL,
    'REF' TEXT NOT NULL,
    'ReviewDate' TEXT NOT NULL,
    'CocoaPercent' REAL NOT NULL,
    'CompanyLocationId' INTEGER NOT NULL,
    'Rating' REAL NOT NULL,
    'BeanType' TEXT NOT NULL,
    'BroadBeanOriginId' INTEGER NOT NULL
    );
    
    '''

    cur.execute(statement)

    statement = '''
    CREATE Table 'Countries'(
    'ID' INTEGER PRIMARY KEY AUTOINCREMENT,
    'Alpha2' TEXT NOT NULL,
    'Alpha3' TEXT NOT NULL,
    'EnglishName' TEXT NOT NULL,
    'Region' TEXT NOT NULL,
    'Subregion' TEXT NOT NULL,
    'Population' INTEGER NOT NULL,
    'Area' REAL
    );

    '''

    cur.execute(statement)
    #
    country = json.loads(open(COUNTRIESJSON).read())
    # print(country)
    for i in country:
        insertion = (None, i['alpha2Code'], i['alpha3Code'], i['name'], i['region'], i['subregion'], i['population'], i['area'])
        statement = 'INSERT INTO "Countries"'
        statement += "VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
        cur.execute(statement, insertion)

    statement = '''
    SELECT ID, EnglishName FROM Countries
    '''
    country_id_list = cur.execute(statement).fetchall()
    # print(country_id_list)
    country_dict = {}
    for i in country_id_list:
        country_dict[i[1]] = i[0]
    # print(country_dict)

    with open(BARSCSV) as csvDataFile:
        csvReader = csv.reader(csvDataFile)
        next(csvReader)
        for row in csvReader:
            # print(row[8])
            CompanyLocationId = country_dict[row[5]]
            # print(CompanyLocationId)
            try:
                BroadBeanOriginId = country_dict[row[8]]
            except:
                BroadBeanOriginId = ""
            insertion = (None, row[0], row[1], row[2], row[3], row[4], CompanyLocationId, row[6], row[7], BroadBeanOriginId)
            statement = 'INSERT INTO "Bars"'
            statement += "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ? ,?)"
            cur.execute(statement, insertion)

    conn.commit()
    conn.close()


create_choc_db()
populate_choc_db()




# Part 2: Implement logic to process user commands
def process_command(command):

    # base_prompt = "Enter command(or help for options): "
    instruction = '''
            
               bars
                Description: Lists chocolate bars, according the specified parameters.
                Parameters:
                sellcountry=<alpha2> | sourcecountry=<alpha2> | sellregion=<name> | sourceregion=<name> [default: none]
                Description: Specifies a country or region within which to limit the results, and also specifies whether to limit by the seller (or manufacturer) or by the bean origin source.
                ratings | cocoa [default: ratings]
                Description: Specifies whether to sort by rating or cocoa percentage
                top=<limit> | bottom=<limit> [default: top=10]
                Description: Specifies whether to list the top <limit> matches or the bottom <limit> matches.

            '''

    feedback = ""

    while True:
        action = input(feedback + "\n" + base_prompt)
        feedback = ""
        words = action.split()
        if len(words) > 0:
            command = words[0]
        else:
            command = None
        if command == "exit":
            print("Exiting...")
            return

        elif command == "list" and len(words) > 1:
            search = words[1]

            if len(words[1]) != 2:
                feedback += "Please enter a two-letter state abbreviation\n"
            else:
                # call the function to search sites
                list_result = get_sites_for_state(words[1])
                count = 0
                for site in list_result:
                    count = count+1
                    print(str(count)+" "+site.__str__())

        elif command == "nearby" and len(words) > 1:
            search = int(words[1])
            if list_result != []:
                if int(words[1]):
                    if int(words[1]) in range(0, len(list_result)):
                        nearby_result = get_nearby_places_for_site(list_result[int(words[1])-1])
                        if len(nearby_result) == 0:
                            feedback += "Nearby sites are not available. Please try another number on the list"
                        else:
                            for i in nearby_result:
                                print(i.__str__())

                else:
                    feedback += "Please pick a site by number.\n"
            else:
                feedback += "Please search for National Sites in a state first. Enter 'list <state abbreviation>'"

        elif command == "map":

            if type(search) == int:
                plot_nearby_for_site(list_result[search-1])

            elif type(search) == str and search != '':
                plot_sites_for_state(search)
                feedback += "Creating a map for National Sites found."
            else:
                feedback += "Please search for National Sites in a state first"

        elif command == "help":
            print(instruction)

        else:
            feedback += "Command not recognized:"+command

    return []
#
#












# def load_help_text():
#     with open('help.txt') as f:
#         return f.read()
#
# # Part 3: Implement interactive prompt. We've started for you!
# def interactive_prompt():
#     help_text = load_help_text()
#     response = ''
#     while response != 'exit':
#         response = input('Enter a command: ')
#
#         if response == 'help':
#             print(help_text)
#             continue

# Make sure nothing runs or prints out when this file is run as a module
# if __name__=="__main__":
#     interactive_prompt()
