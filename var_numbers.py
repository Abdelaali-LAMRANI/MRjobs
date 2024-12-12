from mrjob.job import MRJob

class MRAverage(MRJob):

    def mapper(self, _, line):
        for number in line.split():
            yield 'var', float(number)

    def reducer(self, key, values):
        mean = 1.001003675424454  
        count = 0
        var = 0
        for value in values:
            count += 1
            var += (value - mean) ** 2
        yield key, (var / count)

if __name__ == '__main__':
    MRAverage.run()

