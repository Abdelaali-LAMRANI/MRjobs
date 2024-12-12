from mrjob.job import MRJob

class FilterRatings(MRJob):

    def mapper(self, _, line):
        parts = line.replace('"', '').split()
        if len(parts) >= 2:
            company = parts[0].strip()
            try:
                rating = float(parts[1].strip())
                if company == "Restauran_X":
                    yield "Restauran_X", rating
            except ValueError:
                pass

    def reducer(self, key, values):
        total = 0
        count = 0
        for value in values:
            total += value
            count += 1
        if count > 0:
            average = total / count
            yield key, {"total_reviews": count, "average_rating": average}
if __name__ == "__main__":
    FilterRatings.run()

