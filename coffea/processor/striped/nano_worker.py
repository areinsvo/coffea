import numpy as np
import cloudpickle as cpkl
import pickle as pkl
from coffea import processor
import lz4.frame as lz4f

class Worker(object):

    Columns = ["Jet.pt", "Jet.eta", "Jet.jetId","MET_pt","Electron.pt"]

    def __init__(self, params, bulk, job, database):
        self.Job = job
        self.Params = params
        self.Bulk = bulk
        
        compressed_proc_class = bulk["processor"]
        self.processor_instance = cpkl.loads(compressed_proc_class)
        self.out = self.processor_instance.accumulator.identity()   
        self.First = True
        
    def frame(self, events):
    
        if self.First:
            self.Job.message("Starting dataset")
            self.First = False
        #XX this map will take a lot of the effort in striped to undo what was done during uploading XX
        # XX Once we abstract this away, then the worker class and accumulator can be the same for all processors XX#
        df_items = {}
        df_items['Jet_pt'] = events.Jet.pt
        df_items['Jet_eta'] = events.Jet.eta
        df_items['MET_pt'] = events.MET_pt

        # XX Need first argument to be the number of events. Need a way to get this generically XX
        df = processor.PreloadedDataFrame(events.MET_pt.size, items=df_items)

        self.out += self.processor_instance.process(df)

#        for k in self.out:
#            self.Job.message("Output key = %s"%k)
#            self.Job.message(str(self.out[k].integrate('dataset').values(overflow='all')))
#            self.Job.message("Done processing")
        #XX Could also accumulate here by returning self.out as done below. Can try both to see if it is faster.
        
    def end(self):
        for k in self.out:
            self.Job.message(str(self.out[k].integrate('dataset').values(overflow='all')))
        return { "coffea_accumulator:" : lz4f.compress(pkl.dumps(self.out),compression_level=1)}
        
class Accumulator:
    
    def __init__(self, params, bulk, job_interface, db_interface):
        self.Data = None
                
    def add(self, data):
        if self.Data is None:
            self.Data = data.copy()
        else:
            for k, v in data.items():
                self.Data[k] =  zipadd(self.Data[k],v)
#                self.Data[k] += v
        
    def values(self):
        return self.Data

    def zipadd(a, b):
        out = pickle.loads(lz4f.decompress(a))
        out += pickle.loads(lz4f.decompress(b))
        return lz4f.compress(pickle.dumps(out), compression_level=1)
