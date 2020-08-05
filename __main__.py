from pyspark import SparkContext

with open('example.txt', 'w') as f:
    lines = [
        'first',
        'second line',
        'the third line',
        'then a fourth line'
    ]
    f.write('\n'.join(lines))

sc = SparkContext()
text_rdd = sc.textFile('example.txt')

# Split each file line and then collect the results
text_rdd.map(lambda line: line.split()).collect()

def check_flat_map(x):
    print('checking flat map')
    print(x)
    return x

# While map will return a list with each file line
# as a item, flatMap will apply function to each file line
# and then flat the result
text_rdd.flatMap(lambda line: line.split()).collect()


# RDDs and Key Value Pairs
with open('services.csv', 'w') as f:
    lines = [
        '#EventId,Timestamp,Customer,State,ServiceID,Amount\n',
        '201,10/13/2017,100,NY,131,100.00\n',
        '204,10/18/2017,700,TX,129,450.00\n',
        '202,10/15/2017,203,CA,121,200.00\n',
        '206,10/19/2017,202,CA,131,500.00\n',
        '203,10/17/2017,101,NY,173,750.00\n',
        '205,10/19/2017,202,TX,121,200.00\n'
    ]
    f.writelines(lines)


services = sc.textFile('services.csv')
services.take(2)

services.map(lambda x: x.split(',')).take(3)


# Removing the '#' from #EventId
clean_serv = services.map(
    lambda x: x[1:].split(',') if x[0] == '#' else x.split(',')
)

clean_serv.collect()


# Grab fields State and Amount
state_amt = clean_serv.map(lambda lst: (lst[3], lst[-1]))
state_amt.collect()


# Reduce by State (like a group by)
amt_by_state = state_amt.reduceByKey(
    lambda amt1, amt2: float(amt1) + float(amt2))

amt_by_state.collect()


# Get rid of ('State', 'Amount') item
amt_by_state = amt_by_state.filter(lambda x: x[0] != 'State')
amt_by_state.collect()


# Sort by the amount value
amt_by_state = amt_by_state.sortBy(
    lambda amt: amt[1], ascending=False)

amt_by_state.collect()
