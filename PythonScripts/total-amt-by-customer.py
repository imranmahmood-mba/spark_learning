from pyspark import SparkConf, SparkContext

# Creatae SparkConf and SparkContext variables
conf = SparkConf().setMaster('local').setAppName('Get Total Amount Spent by Customer')
sc = SparkContext(conf=conf)

def splitTxt(line, delimiter=','):
    fields = line.split(delimiter)
    customer_id = fields[0]
    amount = fields[2]
    return (int(customer_id), amount)


input = sc.textFile("../customer-orders.csv")
parsed_lines = input.map(splitTxt)
amount_spent = parsed_lines.reduceByKey(lambda x, y: float(x) + float(y))
flipped_amount_spent = amount_spent.sortBy(lambda x: x[1], ascending=False)

results = amount_spent.collect()
flipped_results = flipped_amount_spent.collect()
for flipped_result in flipped_results:
    print(f"Customer with id: {flipped_result[0]} spent {flipped_result[1]}")
