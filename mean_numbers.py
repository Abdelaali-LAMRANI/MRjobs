from mrjob.job import MRJob

class MRAverage(MRJob):

    def mapper(self, _, line):
        # Yield each number from the line
        for number in line.split():
            try:
                yield 'mean : ', float(number)
            except ValueError:
                pass  # skip non-numeric values

    def reducer(self, key, values):
        # Calculate the sum and count of numbers
        total = 0
        count = 0
        for value in values:
            total += value
            count += 1
        
        # Yield the average
        if count > 0:
            yield key, total / count
        else:
            yield key, 0

if __name__ == '__main__':
    MRAverage.run()
