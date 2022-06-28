import json

class WordMap:
    words = { }
    spamTotal = 0
    hamTotal = 0

    def __init__(self, words={}, hamTotal=0, spamTotal=0):
        self.words = words
        self.hamTotal = hamTotal
        self.spamTotal = spamTotal

    def incrementSpamTotal(self, amount):
        self.spamTotal += amount

    def incrementHamTotal(self, amount):
        self.hamTotal += amount

    def put(self, word, key):
        if key not in self.words:
            self.words[key] = word

    def contains(self, key):
        return key in self.words

    def toJSON(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=False, indent=None)
