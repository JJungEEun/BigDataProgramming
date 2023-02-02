from mrjob.job import MRJob
from mrjob.step import MRStep
from time import localtime

class PopMovie(MRJob):
	def steps(self):
		return [
				MRStep(mapper=self.map_rating_count,
					combiner=self.combine_rating_count,
					reducer=self.reduce_rating_count),
				MRStep(reducer=self.reduce_sort)
				]

	def map_rating_count(self,_,line):
		userid, movieid, rating, timestamp = line.split(',')
		if userid != 'userId':
			yield ((movieid), (float(rating),1))

	def combine_rating_count(self, keys, values):
		totalratings, total = 0, 0
		for value in values:
			totalratings += value[0]
			total += value[1]
		yield ((keys), (totalratings,total))

	def reduce_rating_count(self, keys, values):
		totalratings, total = 0, 0
		for value in values:
			totalratings += value[0]
			total += value[1]
		avg = totalratings/total
		yield(None, (avg, keys))
	
	def reduce_sort(self,_, keys):
		for key in sorted(keys, reverse=True):
			yield (key[1], key[0])

if __name__ =='__main__':
	PopMovie.run()

