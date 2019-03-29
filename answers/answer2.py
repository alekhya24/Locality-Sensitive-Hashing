from pyspark.sql import SparkSession
from pretty_print_dict import pretty_print_dict as ppd
from pretty_print_bands import pretty_print_bands as ppb
import random
# Dask imports
import dask.bag as db
import dask.dataframe as df


all_states = ["ab", "ak", "ar", "az", "ca", "co", "ct", "de", "dc",
              "fl", "ga", "hi", "id", "il", "in", "ia", "ks", "ky", "la",
              "me", "md", "ma", "mi", "mn", "ms", "mo", "mt", "ne", "nv",
              "nh", "nj", "nm", "ny", "nc", "nd", "oh", "ok", "or", "pa",
              "pr", "ri", "sc", "sd", "tn", "tx", "ut", "vt", "va", "vi",
              "wa", "wv", "wi", "wy", "al", "bc", "mb", "nb", "lb", "nf",
              "nt", "ns", "nu", "on", "qc", "sk", "yt", "dengl", "fraspm"]


def init_spark():
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    return spark


def toCSVLineRDD(rdd):
    """This function is used by toCSVLine to convert an RDD into a CSV string

    """
    a = rdd.map(lambda row: ",".join([str(elt) for elt in row]))\
           .reduce(lambda x, y: "\n".join([x, y]))
    return a + "\n"


def toCSVLine(data):
    """This function convert an RDD or a DataFrame into a CSV string

    """
    if isinstance(data, RDD):
        return toCSVLineRDD(data)
    elif isinstance(data, DataFrame):
        return toCSVLineRDD(data.rdd)
    return None

from collections import OrderedDict
all_plant_dict = OrderedDict()

def data_preparation(data_file, key, state):
    """Our implementation of LSH will be based on RDDs. As in the clustering
    part of LA3, we will represent each state in the dataset as a dictionary of
    boolean values with an extra key to store the state name.
    We call this dictionary 'state dictionary'.

    Task 1 : Write a script that
             1) Creates an RDD in which every element is a state dictionary
                with the following keys and values:

                    Key     |         Value
                ---------------------------------------------
                    name    | abbreviation of the state
                    <plant> | 1 if <plant> occurs, 0 otherwise

             2) Returns the value associated with key
                <key> in the dictionary corresponding to state <state>

    *** Note: Dask may be used instead of Spark.

    Keyword arguments:
    data_file -- csv file of plant name/states tuples (e.g. ./data/plants.data)
    key -- plant name
    state -- state abbreviation (see: all_states)
    """
    from collections import OrderedDict
    global all_plant_dict
    spark = init_spark()
    data = spark.sparkContext.textFile(data_file)

    all_plant = data.map(lambda val: val.split(",")[0]).collect()
    all_plant_dict = OrderedDict([(plant, 0) for plant in all_plant])

    data = data.map(lambda val : (val.split(",")[0] , val.split(",")[1:]) ).flatMapValues(lambda val : val)
    data = data.map(lambda val : ( val[1] , [val[0]] )).reduceByKey(lambda x,y : x+y)
    data = data.map(lambda x: ( x[0], {**all_plant_dict, **OrderedDict([(t, 1) for t in x[1]])}  ))

    result = data.filter(lambda val : val[0] == state).map(lambda val : val[1]).collect()
    if ( (key != '') & (state != '')):
        return result[0][key]
    else:
        return data


def primes(n, c):
    """To create signatures we need hash functions (see next task). To create
    hash functions,we need prime numbers.

    Task 2: Write a script that returns the list of n consecutive prime numbers
    greater or equal to c. A simple way to test if an integer x is prime is to
    check that x is not a multiple of any integer lower or equal than sqrt(x).

    Keyword arguments:
    n -- integer representing the number of consecutive prime numbers
    c -- minimum prime number value
    """
    primes = []
    import math
    while True:
        if len(primes) == n:
            break
        l = list(range(2, math.ceil(pow(  c , 1/2  )+1  )))
        result = [False for i in l if (c % i == 0)]
        if False not in result:
            primes.append(c)
        c+=1
    return primes


def hash_plants(s, m, p, x):
    """We will generate hash functions of the form h(x) = (ax+b) % p, where a
    and b are random numbers and p is a prime number.

    Task 3: Write a function that takes a pair of integers (m, p) and returns
    a hash function h(x)=(ax+b)%p where a and b are random integers chosen
    uniformly between 1 and m, using Python's random.randint. Write a script
    that:
        1. initializes the random seed from <seed>,
        2. generates a hash function h from <m> and <p>,
        3. returns the value of h(x).

    Keyword arguments:
    s -- value to initialize random seed from
    m -- maximum value of random integers
    p -- prime number
    x -- value to be hashed
    """
    if s != None:
        random.seed(s)
    a = random.randint(1, m)
    b = random.randint(1, m)
    #print("a : {0}\nb : {1}".format(a,b))
    val = (a*x + b) % p

    return val


def hash_list(s, m, n, i, x):
    """We will generate "good" hash functions using the generator in 3 and
    the prime numbers in 2.

    Task 4: Write a script that:
        1) creates a list of <n> hash functions where the ith hash function is
           obtained using the generator in 3, defining <p> as the ith prime
           number larger than <m> (<p> being obtained as in 1),
        2) prints the value of h_i(x), where h_i is the ith hash function in
           the list (starting at 0). The random seed must be initialized from
           <seed>.

    Keyword arguments:
    s -- seed to intialize random number generator
    m -- max value of hash random integers
    n -- number of hash functions to generate
    i -- index of hash function to use
    x -- value to hash
    """
    from collections import OrderedDict

    hash_function = {}
    for j in range(0, i+2):
        p = primes(j + 1, m)[j]
        if j == 0:
            hash_function[j] = hash_plants(s , m, p , x)
        else:
            hash_function[j] = hash_plants(None, m, p, x)
        #print("For execution {0}, the value of p is : {1}, the result of hash function is : {2}".format(j,p,hash_function[j]))

    return hash_function[i]

signature_rdd = None
signature_rdd_list  = []

def signatures(datafile, seed, n, state):
    """We will now compute the min-hash signature matrix of the states.

    Task 5: Write a function that takes build a signature of size n for a
            given state.

    1. Create the RDD of state dictionaries as in data_preparation.
    2. Generate `n` hash functions as done before. Use the number of line in
       datafile for the value of m.
    3. Sort the plant dictionary by key (alphabetical order) such that the
       ordering corresponds to a row index (starting at 0).
       Note: the plant dictionary, by default, contains the state name.
       Disregard this key-value pair when assigning indices to the plants.
    4. Build the signature array of size `n` where signature[i] is the minimum
       value of the i-th hash function applied to the index of every plant that
       appears in the given state.


    Apply this function to the RDD of dictionary states to create a signature
    "matrix", in fact an RDD containing state signatures represented as
    dictionaries. Write a script that returns the string output of the RDD
    element corresponding to state '' using function pretty_print_dict
    (provided in answers).

    The random seed used to generate the hash function must be initialized from
    <seed>, as previously.

    ***Note: Dask may be used instead of Spark.

    Keyword arguments:
    datafile -- the input filename
    seed -- seed to initialize random int generator
    n -- number of hash functions to generate
    state -- state abbreviation
    """
    spark = init_spark()
    from collections import OrderedDict
    global all_plant_dict, signature_rdd, signature_rdd_list
    #state_rdd = data_preparation(datafile, '', '')
    final_result = {}
    data = data_preparation(datafile, '', '')
    data_dict = OrderedDict(data.collect())
    print(data_dict)
    all_plant = sorted(all_plant_dict.keys())
    m = len(all_plant)

    for s in data_dict.keys():
        temp_dict = {}
        for i in range(0, n):
            temp_dict[i] = 0
        final_result[s] = temp_dict

    prime_list = primes(n, m)
    random.seed(seed)

    for i in range(0, n ):
        a = random.randint(1,m)
        b = random.randint(1,m)
        for s in data_dict.keys():
            l = 0
            plant_dict = data_dict[s]
            l = ([(x*a + b)%prime_list[i] for x in range(len(all_plant)) if plant_dict[all_plant[x]] == 1  ])
            final_result[s][i] = min(l)
    print("This is the result of qc we get : ",final_result['qc'])
    result_rdd = spark.sparkContext.parallelize([val for val in data_dict.keys()])
    result_rdd = result_rdd.map(lambda val : (val, final_result[val]) )
    output = result_rdd.filter(lambda val : val[0] == state).map(lambda x: x[1]).collect()
    signature_rdd = result_rdd
    signature_rdd_list = result_rdd.collect()
    return output[0]


def hash_band(datafile, seed, state, n, b, n_r):
    """We will now hash the signature matrix in bands. All signature vectors,
    that is, state signatures contained in the RDD computed in the previous
    question, can be hashed independently. Here we compute the hash of a band
    of a signature vector.

    Task: Write a script that, given the signature dictionary of state <state>
    computed from <n> hash functions (as defined in the previous task),
    a particular band <b> and a number of rows <n_r>:

    1. Generate the signature dictionary for <state>.
    2. Select the sub-dictionary of the signature with indexes between
       [b*n_r, (b+1)*n_r[.
    3. Turn this sub-dictionary into a string.
    4. Hash the string using the hash built-in function of python.

    The random seed must be initialized from <seed>, as previously.

    Keyword arguments:
    datafile --  the input filename
    seed -- seed to initialize random int generator
    state -- state to filter by
    n -- number of hash functions to generate
    b -- the band index
    n_r -- the number of rows
    """
    global signature_rdd, signature_rdd_list
    if signature_rdd == None:
        print("Calculating Signature matrix")
        signatures(datafile, seed, n, state)

    for s, d in signature_rdd_list:
        if s == state:
            matrix_dict = d

    subdict_string = ''
    index_list = [i for i in range(b*n_r , (b+1)*n_r ) ]
    subdict = dict((key,value) for key,value in matrix_dict.items() if key in index_list)

    return hash(str(subdict))

def hash_bands(data_file, seed, n_b, n_r):
    """We will now hash the complete signature matrix

    Task: Write a script that, given an RDD of state signature dictionaries
    constructed from n=<n_b>*<n_r> hash functions (as in 5), a number of bands
    <n_b> and a number of rows <n_r>:

    1. maps each RDD element (using flatMap) to a list of ((b, hash),
       state_name) tuples where hash is the hash of the signature vector of
       state state_name in band b as defined in 6. Note: it is not a triple, it
       is a pair.
    2. groups the resulting RDD by key: states that hash to the same bucket for
       band b will appear together.
    3. returns the string output of the buckets with more than 2 elements
       using the function in pretty_print_bands.py.

    That's it, you have printed the similar items, in O(n)!

    Keyword arguments:
    datafile -- the input filename
    seed -- the seed to initialize the random int generator
    n_b -- the number of bands
    n_r -- the number of rows in a given band
    """
    spark = init_spark()
    n = n_b*n_r #number of hash function to generate
    global signature_rdd,signature_rdd_list
    if signature_rdd == None:
        signatures(data_file , seed, n, 'qc')

    result = []
    for i in range(0, n_b):
        for state,dic in signature_rdd_list:
            tup = ((i , hash_band(data_file, seed, state, n, i, n_r)),state)
            result.append(tup)

    print("This is the result before groupby : ",result)

    hashed_rdd = spark.sparkContext.parallelize(result)
    h = hashed_rdd.groupByKey().map(lambda x: (x[0], list(x[1]))).filter(lambda x: len(x[1]) > 1)
    result = h.collect()
    return_string = ppb(h)
    print(return_string)
    return return_string


#hash_bands(r"../data/plants.data", 123, 5, 7)

def get_b_and_r(n, s):
    """The script written for the previous task takes <n_b> and <n_r> as
    parameters while a similarity threshold <s> would be more useful.

    Task: Write a script that prints the number of bands <b> and rows <r> to be
    used with a number <n> of hash functions to find the similar items for a
    given similarity threshold <s>. Your script should also print <n_actual>
    and <s_actual>, the actual values of <n> and <s> that will be used, which
    may differ from <n> and <s> due to rounding issues. Printing format is
    found in tests/test-get-b-and-r.txt

    Use the following relations:

     - r=n/b
     - s=(1/b)^(1/r)

    Hint: Johann Heinrich Lambert (1728-1777) was a Swiss mathematician

    Keywords arguments:
    n -- the number of hash functions
    s -- the similarity threshold
    """
    raise Exception("Not implemented yet")
