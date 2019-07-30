import numpy as np

class Worker(object):

    Columns = ["Jet.pt", "Jet.eta", "Jet.jetId"]

    def __init__(self, params, bulk, job, database):
        self.Job = job
        self.Params = params
        self.Bulk = bulk
        
        compressed_processor = bulk["processor"]
        # XX uncompress processor XX Not sure if this should be here or done per frame. Probably here.
        self.processor_instance = compressed_processor.XX_uncompress_XX
        
        self.out = processor_instance.accumulator.identity()
        
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
        
        # XX Need first argument to be the number of events. Need a way to get this generically XX
        df = processor.PreloadedDataFrame(events.Met_pt.counts, items=df_items)

        self.processor_instance.process(df)
            
        #XX Could also accumulate here by returning self.out as done below. Can try both to see if it is faster.
        
    def end(self):
        return { "coffea_accumulator:" :   self.out    }
        
class Accumulator:
    
    def __init__(self, params, bulk, job_interface, db_interface):
        self.Data = None
                
    def add(self, data):
        if self.Data is None:
            self.Data = data.copy()
        else:
            for k, v in data.items():
                self.Data[k] += v
        
    def values(self):
        return self.Data
