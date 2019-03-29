from pyspark.sql import SparkSession
from pretty_print_dict import pretty_print_dict as ppd
from pretty_print_bands import pretty_print_bands as ppb
import random
# Dask imports
import dask.bag as db
import dask.dataframe as df
from pyspark.sql import Row
from pyspark import SparkContext
from pyspark.sql.functions import array_contains,array
from pyspark.sql.functions import col
from collections import OrderedDict
sc = SparkContext()


all_plants_dict = OrderedDict()
states_dict=None

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
    createDict(data_file)
    dict_op=getFromDict(state)
    return dict_op[0][key]

def getFromDict(state):
    dict_op = state_data.filter(lambda val : val[0] == state).map(lambda val : val[1]).collect()
    return dict_op

def createDict(data_file):
    global all_plants_dict
    spark = init_spark()
    lines = spark.sparkContext.textFile(data_file)

    all_plants = lines.map(lambda val: val.split(",")[0]).collect()
    all_plants_dict = OrderedDict([(plant, 0) for plant in all_plants])

    data = lines.map(lambda val : (val.split(",")[0] , val.split(",")[1:]) ).flatMapValues(lambda val : val)	
    data = data.map(lambda val : ( val[1] , [val[0]] )).reduceByKey(lambda x,y : x+y)
    global state_data
    state_data = data.map(lambda x: ( x[0], {**all_plants_dict, **OrderedDict([(t, 1) for t in x[1]])}  ))
    

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
    possiblePrime=c
    while(True):
    # Assume number is prime until shown it is not. 
        isPrime = True
        for num in range(2, int(possiblePrime ** 0.5) + 1):
            if possiblePrime % num == 0:
                isPrime = False
                break
      
        if isPrime and possiblePrime>=c and len(primes)<n:
            primes.append(possiblePrime)
        if(len(primes)==n):
            break
        possiblePrime+=1
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
    if s!=None:
        random.seed(s)
    a = random.randint(1,m)
    b=random.randint(1,m)
    hash_value = (a*x + b)%p
    return hash_value


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
    get_primes =primes(n,m)
    ith_prime_number=get_primes[i]
    hash_values=create_hash_list(get_primes,s,m,n,x)   
    return hash_values[i]

def create_hash_list(prime_list,s,m,n,x):
    hash_values=[]
    for j in range(0,n):
        if j==0:
            hash_values.append(hash_plants(s,m,prime_list[j],x))
        else:
            hash_values.append(hash_plants(None,m,prime_list[j],x))
    return hash_values
    

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
    createDict(datafile)
    createminHash(n,seed)
    op_dict=states_dict[state]
    op=ppd(op_dict)
    print(op)
    return op_dict

def createminHash(n,seed):
    global states_dict
    states_dict={}
    data_dict = OrderedDict(state_data.collect())
    final_result={}
    for s in data_dict.keys():
        temp_dict = {}
        for i in range(0, n):
            temp_dict[i] = 0
        states_dict[s] = temp_dict
    all_plants=sorted(all_plants_dict.keys())
    m=len(all_plants)
    get_primes=primes(n,m)
    random.seed(seed)
    for i in range(0,n):
        hash_op={}
        a=random.randint(1,m)
        b=random.randint(1,m)
        for state in data_dict.keys():
            op=0
            dict_op=data_dict[state]            
            p=get_primes[i]
            op = ([(a*index + b)%p for index in range(len(all_plants)) if dict_op[all_plants[index]] == 1  ])
            states_dict[state][i] = min(op)

def hash_band(datafile, seed, state, n, b, n_r):
    """We will now hash the signature matrix in bands. All signature vectors,
    that is, state signatures contained in the RDD computed in the previous
    question, can be hashed independently. Here we compute the hash of a band
    of a signature vec
    tor.

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
    createDict(datafile)
    createminHash(n,seed)
    op =getHashBand(state,b,n_r)
    return op

def getHashBand(state,b,n_r):
    hash_band_dict=states_dict[state]
    sub_dict=dict((k,hash_band_dict[k]) for k in range(b*n_r,(b+1)*n_r) if k in hash_band_dict)
    sub_dict_str=str(sub_dict)
    return hash(sub_dict_str)


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
    n=n_b *n_r
    createDict(data_file)
    createminHash(n,seed)
    tuple_op=[]
    for i in range(0,n_b):
        for k,v in states_dict.items():
            tuple_data= ((i,getHashBand(k,i,n_r)),k)
            tuple_op.append(tuple_data)
    rdd = sc.parallelize(tuple_op)
    group_rdd=rdd.groupByKey().map(lambda x:(x[0],list(x[1])))
    final_op=group_rdd.filter(lambda x: len(x[1]) > 1)
    op=ppb(final_op)
    return op
    

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
