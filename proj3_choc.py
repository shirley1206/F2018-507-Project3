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


# create_choc_db()
# populate_choc_db()
#


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

        if 1 < len(words) <= 4:
            base_statement = '''
            SELECT b.SpecificBeanBarName,b.Company,c1.EnglishName,b.Rating,b.CocoaPercent,C2.EnglishName from Bars as b
            JOIN Countries as c1
            on c1.ID = b.CompanyLocationId
            JOIN Countries as c2
            on c2.ID = b.BroadBeanOriginId
            '''

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
                order_statement = 'ORDER BY b.rating DESC'

            else:
                order_statement = 'ORDER BY b.CocoaPercent DESC'

            for option in words:

                if 'top=' in option:
                    value = option.split("=")[1]
                    limit_statement = 'LIMIT '
                    limit_statement += value

                elif 'bottom=' in option:
                    value = option.split("=")[1]
                    order_statement.replace('DESC ', 'ASC ')
                    limit_statement = 'LIMIT '
                    limit_statement += value

            if 'top=' not in words and 'bottom=' not in words:
                limit_statement = 'LIMIT 10'

            final_statement = base_statement+filter_statement+' '+order_statement+' '+limit_statement
            result = cur.execute(final_statement).fetchall()
            print('bars')

            return result


    elif "companies" == words[0]:
        if 1 < len(words) <= 4:

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

                elif 'bottom=' in option:
                    value = option.split("=")[1]
                    order_statement.replace('DESC ', 'ASC ')
                    limit_statement = 'LIMIT '
                    limit_statement += value

            if 'top=' not in words and 'bottom=' not in words:
                limit_statement = 'LIMIT 10'

            base_statement = "SELECT b.Company,c.EnglishName,{}".format(agg)
            base_statement += "from Bars as b "
            base_statement += "JOIN Countries as c "
            base_statement += "on c.ID = b.CompanyLocationId "
            group_statement = "Group by b.company Having count (b.company) >4"
            final_statement = base_statement+' '+filter_statement+' '+group_statement+' '+order_statement+' '+limit_statement
            # print(final_statement)
            result = cur.execute(final_statement).fetchall()
            print('companies')
            return result


    elif "countries" == words[0]:
        print('countries')

        if 1 < len(words) <= 4:
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

                elif 'bottom=' in option:
                    value = option.split("=")[1]
                    order_statement.replace('DESC ', 'ASC ')
                    limit_statement = 'LIMIT '
                    limit_statement += value

            if 'top=' not in words and 'bottom=' not in words:
                limit_statement = 'LIMIT 10'

            base_statement = "SELECT c.EnglishName,c.Region,{}".format(agg)
            base_statement += "from Countries as c "
            group_statement = "Group by c.EnglishName Having count (b.CompanyLocationId) >4"
            final_statement = base_statement+join_statement+' '+filter_statement+' '+group_statement+' '+order_statement+' '+limit_statement
            # print(final_statement)
            result = cur.execute(final_statement).fetchall()
            return result






    conn.commit()


result = process_command('countries bars_sold')

print(result)








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
