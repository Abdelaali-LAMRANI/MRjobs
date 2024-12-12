from mrjob.job import MRJob

class MRAverage(MRJob):

    def mapper(self, _, line):
        # Supprimer les guillemets et diviser en parties
        parts = line.replace('"', '').split()
        if len(parts) >= 3:
            try:
                key = parts[1]  # "homme" ou "femme"
                value = float(parts[2])  # Exemple : 7500.0
                yield key, value
            except ValueError:
                pass  # Ignorer les lignes avec des valeurs non numériques

    def reducer(self, key, values):
        # Calculer la moyenne des valeurs pour chaque clé
        total = 0
        count = 0
        for value in values:
            total += value
            count += 1
        if count > 0:
            yield key, total / count

if __name__ == '__main__':
    MRAverage.run()
