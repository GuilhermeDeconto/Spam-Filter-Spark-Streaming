# importing spark related libraries
from pyspark import SparkContext
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql.types import *
from pyspark.sql.functions import *
import json
from tqdm import tqdm
from word import Word
from wordMap import WordMap

def finalizeTraining(wordMap):
    for key in wordMap.words:
        wordMap.words[key].calculateProbability()

def splitWords(content, id):
    tokens = []
    buffer = ""

    for i in range(len(content)):
        char = content[i]

        if char.isalpha() and char.isascii():
            buffer += char.lower()
        elif len(buffer) > 0:
            tokens.append((id, buffer))
            buffer = ""

    if len(buffer) > 0:
        tokens.append((id, buffer))

    return tokens

def trainModel(content, isSpam, wordMap):
    tokens = splitWords(content)

    for token in tokens:
        if len(token) <= 1:
            continue

        word = token
        if wordMap.contains(word):
            w = wordMap.words[word]
        else:
            w = Word(word)
            wordMap.put(w, word)

        if isSpam:
            w.spamFrequency += 1
            wordMap.incrementSpamTotal(1)
        else:
            w.hamFrequency += 1
            wordMap.incrementHamTotal(1)

def saveTrainingAsString(path, wordMap):
    with open(path, 'w') as f:
        f.write(wordMap.toJSON())

def splitData(input):
    values = json.loads(input)
    list = []

    if type(values) == dict:
        list.append(json.dumps(values))
        return list
    else:
        for item in values:
            list.append(json.dumps(item))

        return list

def tokenizerStep(input):
    if len(input) <= 0:
        return ()

    parsed = json.loads(input)
    id = parsed["id"]
    messageId = f"Message ID: {id}"
    label = parsed["label"]
    expected = f"Expected classification: {label}"
    tokens = splitWords(parsed["message"], id)
    
    return (messageId, tokens, expected)

def wordSpamlinessEvaluationStep(word):
    global loadedModel

    messageId = word[0]
    search = word[1]

    if not loadedModel.contains(str(search)): 
        return (messageId, 0)

    overallMessageSpamProbability = 0.8
    overallMessageHamProbability = 1 - overallMessageSpamProbability

    wordSpamProbability = loadedModel.words[str(search)]["spamProbability"]
    wordHamProbability = 1 - wordSpamProbability

    wordSpamliness = (wordSpamProbability * overallMessageSpamProbability) / (wordSpamProbability * overallMessageSpamProbability + wordHamProbability * overallMessageHamProbability)

    return (messageId, wordSpamliness)
    

def messageSpamDeterminationStep(tuples):
    threshold = 1

    if tuples[1][0] + tuples[1][1] <= 0:
        return (tuples[0], "Classification: Invalid")
    messageSpamProbability = tuples[1][0] / (tuples[1][0] + tuples[1][1])
    if messageSpamProbability >= threshold:
        return (tuples[0], "Classification: SPAM")
    else:
        return (tuples[0], "Classification: HAM")

def startTraining():
    wordMap = WordMap()
    path = "/home/ubuntu/Spam-Filter-Spark-Streaming/datasets"
    file = open(f'{path}/output.json', 'r')
    lines = file.readlines()
    print(f"Found {len(lines)} e-mails")
    file.close()

    for line in tqdm(lines):
        clearLine = json.loads(line)
        isSpam = clearLine['label'] == 'spam'
        content = clearLine['message']
        trainModel(content, isSpam, wordMap)

    finalizeTraining(wordMap)

    saveTrainingAsString(f"{path}/fullTraining.json", wordMap)

if __name__ == '__main__':
    isTraining = False
    if (isTraining):
        startTraining()

    with open("/home/ubuntu/Spam-Filter-Spark-Streaming/datasets/fullTraining.json") as json_file:
        parsed = json.load(json_file)
        loadedModel = WordMap(**parsed)

    hostname = "localhost"
    port = 6100

    spark_context = SparkContext.getOrCreate()
    spark = SparkSession(spark_context)
    ssc = StreamingContext(spark_context, 1)

    data = ssc.socketTextStream(hostname, int(port))

    separatedData = data.flatMap(splitData)

    messages = separatedData.map(tokenizerStep).filter(lambda tuple: len(tuple) > 1 and len(tuple[1]) > 2)

    # ids = messages.map(lambda x: x[0])
    # labels = messages.map(lambda x: x[2])
    tokensWithId = messages.map(lambda x: (x[0], x[1]))
    tokens = tokensWithId.flatMap(lambda x: x[1])

    evaluation = tokens.map(wordSpamlinessEvaluationStep).filter(lambda x: x[1] > 0)

    tuples = evaluation.map(lambda x: (x[0],(x[1], (1 - x[1])))).reduceByKey(lambda a, b: (a[0] * b[0], a[1] * b[1]) ).map(messageSpamDeterminationStep)

    tuples.pprint()

    ssc.start()
    ssc.awaitTermination()