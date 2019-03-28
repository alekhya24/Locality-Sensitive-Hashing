from pyspark.sql import SparkSession
from pretty_print_dict import pretty_print_dict as ppd
from pretty_print_bands import pretty_print_bands as ppb
import random
# Dask imports
import dask.bag as db
import dask.dataframe as df
from pyspark.sql import Row
from pyspark import SparkContext
from pyspark.ml.linalg import SparseVector
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import array_contains,array
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import col
import numpy as np
import itertools
sc = SparkContext()


data_df=None
data_f=None
all_plants=None
state_dict_rdd=None
parts=None
all_plants_indexed=None

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
    spark = init_spark()
    lines = spark.read.text(data_file).rdd
    global parts
    parts= lines.map(lambda row: row.value.split(","))
    rdd_data = parts.map(lambda p: Row(plant_name=p[0], states=p[1:]))
    global data_df                         
    data_df = spark.createDataFrame(rdd_data)
    data_df.cache()
    rdd=createDict(data_df)
    global data_f
    data_f = spark.createDataFrame(rdd)
    data_f.cache()
    print(data_f)
    dict_op=getFromDict(key)
    row = Row(**dict_op[0][0])
    return row.asDict()[state]

def getFromDict(key):
    dict_op = data_f.select(data_f._2).where(data_f._1==key).collect()
    return dict_op

def createDict(data):
    dict_list=[()]
    for iteration in data.collect():
        plant_states=iteration.states
        dict1= dict( [ (state,1) if state in plant_states  else (state,0) for state in all_states] )
        tuple_data=(iteration.plant_name,dict1)
        dict_list.append(tuple_data)
    global state_dict_rdd
    state_dict_rdd = sc.parallelize(dict_list[1:])
    return state_dict_rdd


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
    spark = init_spark()
    lines = spark.read.option("encoding", "ISO-8859-1").text(datafile).rdd
    parts= lines.map(lambda row: row.value.split(","))
    m=lines.count()
    get_primes=primes(n,m)
    random.seed(seed)
    plants_df = createMatrix(parts)
    '''ip=plants_df.where(plants_df._1==state).select(plants_df._2).rdd.flatMap(lambda x:x).collect()
    flattened_list = list(itertools.chain(*ip))'''
    '''op = plants_df.mapValues(get_signMatrix)'''
    dict_op=getFromPlantDict(state)
    row = Row(**dict_op[0][0])
    op_dict={}
    for i in range(0,n):
            a=random.randint(1,m)
            b=random.randint(1,m)
            p=get_primes[i]
            op = minhash(row,a,b,p)
            op_dict[i]=op
    return ppd(op_dict)

def getFromPlantDict(key):
    plants_dict_op = plants_data_f.select(plants_data_f._2).where(plants_data_f._1==key).collect()
    return plants_dict_op

def createMatrix(parts):
    spark = init_spark()
    plants_rdd_data = parts.map(lambda p: Row(plant_name=p[0], states=p[1:]))
    global plants_data_df
    plants_data_df = spark.createDataFrame(plants_rdd_data)
    plants_data_df.cache()
    global all_plants_indexed
    
    sorted_plants=plants_data_df.withColumn('plant_name', regexp_replace('plant_name', '×', ''))
    '''.sort(col("plant_name"))'''
    all_plants_indexed= sorted_plants.withColumn("id", monotonically_increasing_id())
    all_plants = all_plants_indexed.select(all_plants_indexed.plant_name).rdd.flatMap(lambda x: x).collect()
    rdd=create_Plants_Dict(all_plants_indexed,all_plants)
    global plants_data_f
    plants_data_f =spark.createDataFrame(rdd)
    return plants_data_f

def create_Plants_Dict(plants_df,all_plants):
    dict_list=[()]
    dict1={}
    for state in all_states:
        plant_data = plants_df.where(array_contains(plants_df.states,state)).collect()
        for plant_name in all_plants:
            for data in plant_data:
                if data.plant_name==plant_name:
                    dict1[data.id]=1
                else:
                    dict1[data.id]=0
        '''dict1= dict([ (plant_name,1) if plant_name in plant_names  else (plant_name,0) for plant_name in all_plants])'''
        op=(state,dict1)
        dict_list.append(op)
    rdd = sc.parallelize(dict_list[1:])
    return rdd

def minhash(data,a,b,p):
    min_value=[]
    row_dict=data.asDict()
    print(row_dict)
    trueIndex=0;
    falseIndex=0;
    for value in all_plants_indexed.collect():
        index=value.id
        if(row_dict[index]!=0):
            hash_value = (a*index + b)%p
            min_value.append(hash_value)
    return min(min_value)

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
    spark = init_spark()
    lines = spark.read.text(datafile).rdd
    parts= lines.map(lambda row: row.value.split(","))
    m=lines.count()
    get_primes=primes(n,m)
    random.seed(seed)
    plants_df = createMatrix(parts)
    dict_op=getFromPlantDict(state)
    row = Row(**dict_op[0][0])
    sign_dict={}
    for i in range(0,n):
        a=random.randint(1,m)
        b=random.randint(1,m)
        p=get_primes[i]
        op = minhash(row,a,b,p)
        sign_dict[i]=op
    sub_dict=dict([(k,sign_dict[k]) for k in range(b*n_r,(b+1)*n_r) if k in sign_dict])
    sub_dict_str=str(sub_dict)
    raise hash(sub_dict_str)


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
    raise Exception("Not implemented yet")


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
