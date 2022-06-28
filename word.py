import json

class Word:
    def __init__(self, word):
        self.word = word
        self.spamFrequency = 0
        self.hamFrequency = 0
        self.spamProbability = 0

    def calculateProbability(self):
        self.spamProbability = self.spamFrequency / self.calculateTotalFrequency()

    def calculateTotalFrequency(self):
        return self.spamFrequency + self.hamFrequency

    def toJSON(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=False, indent=None)
