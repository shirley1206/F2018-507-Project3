import csv
import json
import sqlite3 as sqlite
import sqlite3

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
    'Company' TEXT,
    'SpecificBeanBarName' TEXT,
    'REF' TEXT,
    'ReviewDate' TEXT,
    'CocoaPercent' REAL,
    'CompanyLocationId' INTEGER,
    'Rating' REAL,
    'BeanType' TEXT,
    'BroadBeanOriginId' INTEGER 
    );
    
    '''

    cur.execute(statement)

    statement = '''
    CREATE Table 'Countries'(
    'ID' INTEGER PRIMARY KEY AUTOINCREMENT,
    'Alpha2' TEXT,
    'Alpha3' TEXT,
    'EnglishName' TEXT,
    'Region' TEXT,
    'Subregion' TEXT,
    'Population' INTEGER,
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

            cocoapercentage = float(row[4].strip('%'))/100
            insertion = (None, row[0], row[1], row[2], row[3], cocoapercentage, CompanyLocationId, row[6], row[7], BroadBeanOriginId)
            statement = 'INSERT INTO "Bars"'
            statement += "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ? ,?)"
            cur.execute(statement, insertion)

    conn.commit()
    conn.close()


create_choc_db()
populate_choc_db()



#
# # Part 2: Implement logic to process user commands
def process_command(command):

    conn = sqlite.connect(DBNAME)
    cur = conn.cursor()
    words = command.split()
    # if len(words) > 0:

    if "exit" in words:
        print("Exiting...")
        return

    elif "bars" == words[0]:

        if 1 < len(words):

            filter_statement = ''
            for option in words:

                if 'sellcountry=' in option:
                    value = option.split("=")[1]
                    filter_statement = 'Where c1.Alpha2='
                    filter_statement += '"{}"'.format(value)

                elif 'sourcecountry=' in option:
                    value = option.split("=")[1]
                    filter_statement = 'Where c2.Alpha2='
                    filter_statement += '"{}"'.format(value)

                elif 'sellregion=' in option:
                    value = option.split("=")[1]
                    filter_statement = 'Where c1.Region='
                    filter_statement += '"{}"'.format(value)

                elif 'sourceregion=' in option:
                    value = option.split("=")[1]
                    filter_statement = 'Where c2.Region='
                    filter_statement += '"{}"'.format(value)

            if 'cocoa' in words:
                order_statement = 'ORDER BY b.CocoaPercent DESC'

            else:
                order_statement = 'ORDER BY b.rating DESC'

            for option in words:

                if 'top=' in option:
                    value = option.split("=")[1]
                    limit_statement = 'LIMIT '
                    limit_statement += value

                if 'bottom=' in option:
                    value = option.split("=")[1]
                    order_statement = order_statement.replace(' DESC', ' ASC')
                    limit_statement = 'LIMIT '
                    limit_statement += value

                elif 'top=' not in option and 'bottom=' not in option:
                    limit_statement = 'LIMIT 10'

            base_statement = "SELECT b.SpecificBeanBarName,b.Company,c1.EnglishName,b.Rating,b.CocoaPercent,c2.EnglishName "
            base_statement += "from Bars as b "
            base_statement += "JOIN Countries as c1 "
            base_statement += "on c1.ID = b.CompanyLocationId "
            base_statement += "LEFT JOIN Countries as c2 "
            base_statement += "on c2.ID = b.BroadBeanOriginId "

            final_statement = base_statement+filter_statement+' '+order_statement+' '+limit_statement
            # print(final_statement)
            result = cur.execute(final_statement).fetchall()

            return result


    elif "companies" == words[0]:
        if 1 < len(words):

            filter_statement = ''
            for option in words:

                if 'country=' in option:
                    value = option.split("=")[1]
                    filter_statement = 'Where c.Alpha2='
                    filter_statement += '"{}"'.format(value)

                elif 'region=' in option:
                    value = option.split("=")[1]
                    filter_statement = 'Where c.Region='
                    filter_statement += '"{}"'.format(value)

            if 'cocoa' in words:
                order_statement = 'ORDER BY AVG(b.CocoaPercent) DESC'
                agg = 'round(AVG(b.CocoaPercent),-1)'

            elif 'bars_sold' in words:
                order_statement = 'ORDER BY count(b.company) DESC'
                agg = 'count(b.company)'

            else:
                order_statement = 'ORDER BY AVG(b.rating) DESC'
                agg = 'round(AVG(b.rating),1)'

            for option in words:

                if 'top=' in option:
                    value = option.split("=")[1]
                    limit_statement = 'LIMIT '
                    limit_statement += value

                if 'bottom=' in option:
                    value = option.split("=")[1]
                    order_statement = order_statement.replace(' DESC', ' ASC')
                    limit_statement = 'LIMIT '
                    limit_statement += value

                elif 'top=' not in option and 'bottom=' not in option:
                    limit_statement = 'LIMIT 10'

            base_statement = "SELECT b.Company,c.EnglishName,{}".format(agg)
            base_statement += "from Bars as b "
            base_statement += "JOIN Countries as c "
            base_statement += "on c.ID = b.CompanyLocationId "
            group_statement = "Group by b.company Having count (b.company) >4"
            final_statement = base_statement+' '+filter_statement+' '+group_statement+' '+order_statement+' '+limit_statement
            # print(final_statement)
            result = cur.execute(final_statement).fetchall()
            return result


    elif "countries" == words[0]:
        # print('countries')

        if 1 < len(words):
            filter_statement = ''
            join_statement = "JOIN Bars as b "
            for option in words:

                if 'region=' in option:
                    value = option.split("=")[1]
                    filter_statement = 'Where c.Region='
                    filter_statement += '"{}"'.format(value)

            if 'sources' in words:
                join_statement += "on c.ID = b.BroadBeanOriginId"

            else:
                join_statement += "on c.ID = b.CompanyLocationId"

            if 'cocoa' in words:
                order_statement = 'ORDER BY AVG(b.CocoaPercent) DESC'
                agg = 'round(AVG(b.CocoaPercent),-1)'

            elif 'bars_sold' in words:
                order_statement = 'ORDER BY count(b.CompanyLocationId) DESC'
                agg = 'count(b.CompanyLocationId)'

            else:
                order_statement = 'ORDER BY AVG(b.rating) DESC'
                agg = 'round(AVG(b.rating),1)'

            for option in words:

                if 'top=' in option:
                    value = option.split("=")[1]
                    limit_statement = 'LIMIT '
                    limit_statement += value

                if 'bottom=' in option:
                    value = option.split("=")[1]
                    order_statement = order_statement.replace(' DESC', ' ASC')
                    limit_statement = 'LIMIT '
                    limit_statement += value

                elif 'top=' not in option and 'bottom=' not in option:
                    limit_statement = 'LIMIT 10'

            base_statement = "SELECT c.EnglishName,c.Region,{}".format(agg)
            base_statement += "from Countries as c "
            group_statement = "Group by c.EnglishName Having count (b.CompanyLocationId) >4"
            final_statement = base_statement+join_statement+' '+filter_statement+' '+group_statement+' '+order_statement+' '+limit_statement
            # print(final_statement)
            result = cur.execute(final_statement).fetchall()
            return result


    elif "regions" == words[0]:
        # print('regions')

        if 1 < len(words):
            filter_statement = ''
            join_statement = "JOIN Bars as b "

            if 'sources' in words:
                join_statement += "on c.ID = b.BroadBeanOriginId"

            else:
                join_statement += "on c.ID = b.CompanyLocationId"

            if 'cocoa' in words:
                order_statement = 'ORDER BY AVG(b.CocoaPercent) DESC'
                agg = 'round(AVG(b.CocoaPercent),-1)'

            elif 'bars_sold' in words:
                order_statement = 'ORDER BY count (c.Region) DESC'
                agg = 'count (c.Region)'

            else:
                order_statement = 'ORDER BY AVG(b.rating) DESC'
                agg = 'round(AVG(b.rating),1)'

            for option in words:

                if 'top=' in option:
                    value = option.split("=")[1]
                    limit_statement = 'LIMIT '
                    limit_statement += value

                if 'bottom=' in option:
                    value = option.split("=")[1]
                    order_statement = order_statement.replace(' DESC', ' ASC')
                    limit_statement = 'LIMIT '
                    limit_statement += value

                elif 'top=' not in option and 'bottom=' not in option:
                    limit_statement = 'LIMIT 10'

            base_statement = "SELECT c.Region,{}".format(agg)
            base_statement += "from Countries as c "
            group_statement = "Group by c.Region Having count (c.Region) >4"
            final_statement = base_statement+join_statement+' '+filter_statement+' '+group_statement+' '+order_statement+' '+limit_statement
            # print(final_statement)
            result = cur.execute(final_statement).fetchall()
            return result

    conn.commit()


def load_help_text():
    with open('help.txt') as f:
        return f.read()


# Part 3: Implement interactive prompt. We've started for you!
def interactive_prompt():
    help_text = load_help_text()
    response = ''
    while response != 'exit':
        response = input('Enter a command: ')

        if response == 'help':
            print(help_text)
            continue

        if response == 'exit':
            print("bye")
            continue

        if response == '':
            continue

        words = response.split()
        command_list = ["bars", "countries", "companies", "regions", "sellcountry","sourcecountry", "sellregion", "sourceregion", "ratings", "cocoa", "bars_sold", "top", "bottom", "country", "region", "sellers", "sources"]

        if len(words) >= 2:

            for command in words:
                param = command.split("=")[0]

                if param in command_list:
                    result = process_command(response)

                if param not in command_list:
                    result = ''
                    continue

            if result != '':

                for row in result:
                    string_list = []
                    for data in row:
                        if isinstance(data, float) and data < 1.0:
                            data = data * 100
                            data = round(data)
                            data = str(data)
                            data = data + '%'
                        string_list.append(data)
                    # print(string_list)
                    truncated_string_list = []
                    for data in string_list:
                        data = str(data)
                        data = data.replace('None', 'Unknown')

                        if len(data) > 12:
                            t_data = data[:12]+'...'
                            truncated_string_list.append(t_data)

                        else:
                            truncated_string_list.append(data)

                    # print(truncated_string_list)
                    if len(truncated_string_list) == 6:
                        if truncated_string_list[4] == '1.0':
                            truncated_string_list[4] = '100%'
                        output = "{:15} {:15} {:15} {:5} {:5} {:15}".format(truncated_string_list[0], truncated_string_list[1], truncated_string_list[2], truncated_string_list[3], truncated_string_list[4], truncated_string_list[5])
                        print(output)

                    if len(truncated_string_list) == 3:
                        output = "{:15} {:15} {:5}".format(truncated_string_list[0], truncated_string_list[1], truncated_string_list[2])
                        print(output)

                    if len(truncated_string_list) == 2:
                        output = "{:15} {:5}".format(truncated_string_list[0], truncated_string_list[1])
                        print(output)






            else:
                print("Command not recognized:"+' '+response)


        else:
            print("Command not recognized:"+' '+response)


# Make sure nothing runs or prints out when this file is run as a module
if __name__=="__main__":
    interactive_prompt()
