
from mrjob.job import MRJob
from mrjob.step import MRStep

class MRLinesCount(MRJob):
    def steps(self):
        return [
            MRStep(
                mapper=self.mapper,
                combiner=self.combiner,
                reducer=self.reducer1,
            ),
            MRStep(reducer=self.reducer2)
        ]

    def mapper(self, _, line):
        splitted = line.split('\" \"')
        character = splitted[1]

        yield (character, 1)

    def combiner(self, character, counts):
        yield (character, sum(counts))

    def reducer1(self, character, count):
        yield None, (sum(count), character)

    def reducer2(self, _, cnt_character):
        for cnt, character in sorted(cnt_character, reverse=True)[:20]:
            yield character, cnt
        
if __name__ == "__main__":
    MRLinesCount.run()
