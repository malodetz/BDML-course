
from mrjob.job import MRJob
from mrjob.step import MRStep
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.util import ngrams

import nltk

class MRBigramsCount(MRJob):
    def steps(self):
        return [
            MRStep(
                mapper_init=self.mapper_init,
                mapper=self.mapper,
                combiner=self.combiner,
                reducer=self.reducer1,
            ),
            MRStep(reducer=self.reducer2)
        ]
        
    def mapper_init(self):
        nltk.download('punkt')
        nltk.download('stopwords')

    def mapper(self, _, line):
        splitted = line.split('\" \"')
        line = splitted[-1].strip()[:-1]
        tokenized = word_tokenize(line.lower())
        stop_words = set(stopwords.words('english'))
        filtered_sentence = [w for w in tokenized if (not w in stop_words) and  w.isalnum()]
        for bigram in ngrams(filtered_sentence, 2):
            yield (" ".join(bigram), 1)

    def combiner(self, bigram, counts):
        yield (bigram, sum(counts))

    def reducer1(self, bigram, counts):
        yield None, (sum(counts), bigram)

    def reducer2(self, _, cnt_bigram):
        for cnt, bigram in sorted(cnt_bigram, reverse=True)[:20]:
            yield bigram, cnt
        
if __name__ == "__main__":
    MRBigramsCount.run()
