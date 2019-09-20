import os
from striped.job import Session
import numpy as np
from tqdm import tqdm
import cloudpickle as cpkl
import pickle as pkl
import lz4.frame as lz4f
from coffea import hist, processor

class AnalysisProcessor(processor.ProcessorABC):
    def __init__(self):
        self._accumulator = processor.dict_accumulator({
                'fj1pt': hist.Hist("Events", hist.Cat("dataset", "Primary dataset"), hist.Bin("fj1pt","AK15 Leading Jet Pt",[200.0, 250.0, 280.0, 310.0, 340.0, 370.0, 400.0, 430.0, 470.0, 510.0, 550.0, 590.0, 640.0, 690.0, 740.0, 790.0, 840.0, 900.0, 960.0, 1020.0, 1090.0, 1160.0, 1250.0]))
                })

    @property
    def accumulator(self):
        return self._accumulator

    def process(self, df):
        hout = self.accumulator.identity()
        hout['fj1pt'].fill(dataset="Test",fj1pt=df["Jet_pt"].max().flatten())        
        return hout

    def postprocess(self, accumulator):
        return accumulator


class CoffeaDataCallback:
    
    def __init__(self, temp_acc):
        self.coffea_accumulator = pkl.dumps(temp_acc)
        self.count = 0
    
    def on_data(self, wid, nevents, data):
        print("on_data being called")
        out =  pkl.loads(self.coffea_accumulator)
        out += pkl.loads(data["coffea_accumulator"])
        self.coffea_accumulator = pkl.dumps(out)
        self.count += 1
        print("finished with on_data")

    def on_job_finish(self, nsamples, error):
        nsamples = nsamples
#        self.coffea_accumulator += data["coffea_accumulator"]

def run_striped_job(datasets, session_name, proc_instance):
    session = Session(session_name)
        
    processor_instance = cpkl.loads(proc_instance)
 #   final_accumulator =processor_instance.accumulator
    temp_acc = processor_instance.accumulator.identity()
    data_collector = CoffeaDataCallback(temp_acc)

    for ds in datasets:
        job = session.createJob(ds,
                            user_params = {"dataset":ds},
                            bulk_data = {"processor":proc_instance},
                            callbacks = [data_collector],
                            worker_class_file="nano_worker.py")
        job.run()
        final_accumulator = pkl.loads(data_collector.coffea_accumulator)
#        final_accumulator += processor_instance.postprocess(pkl.loads(lz4f.decompress(data_collector.coffea_accumulator)))
 
    print(type(final_accumulator))

    print("Count = ",data_collector.count)

    for k in final_accumulator:
        print("Output key = %s"%k)
        print(final_accumulator[k])
        print(final_accumulator[k].integrate('dataset').values(overflow='all'))
    print("Done")
    return final_accumulator

session_name = "striped.yaml"
dataset = ["NanoTuples-2016_QCD_HT1500to2000_TuneCUETP8M1_13TeV-madgraphMLM-pythia8"]

proc_instance = AnalysisProcessor()
proc_class = cpkl.dumps(proc_instance)
np_array = np.frombuffer(proc_class,dtype="b")
run_striped_job(dataset, session_name, np_array)   
