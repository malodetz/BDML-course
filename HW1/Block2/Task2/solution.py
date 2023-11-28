
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
        line = splitted[-1].strip()[:-1]

        yield (character, (len(line), line))

    def combiner(self, character, lines):
        yield (character, max(lines))

    def reducer1(self, character, lines):
        yield None, (max(lines), character)

    def reducer2(self, _, lines_character):
        for line, character in sorted(lines_character, reverse=True):
            if "dialogue" not in character:   
                yield character, line[1]
        
if __name__ == "__main__":
    MRLinesCount.run()
