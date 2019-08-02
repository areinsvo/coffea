import os
from striped.job import Session
import numpy as np

class CoffeaDataCallback:
    
    def __init__(self):
        self.coffea_accumulator = {}
    
    def on_data(self, wid, nevents, data):
        self.coffea_accumulator += data["coffea_accumulator"]
    
    def on_job_finish(self, nsamples, error):
        self.coffea_accumulator += data["coffea_accumulator"]

def run_striped_job(datasets, session_name, proc_instance):
    data_collector = CoffeaDataCallback()
    session = Session(session_name)
    
    # XX compress proc_instance to compressed_proc_instance
    
    for ds in datasets:
        job = session.createJob(ds,
                            user_params = {"dataset":ds},
                                bulk_data = {},#XX pass compressed_proc_instance here XX
                            callbacks = [data_collector],
                            worker_class_file="nano_worker.py")
        job.run()
        final_accumulator = proc_instance.post_process(data_collector.coffea_accumulator)
        for k in final_accumulator:
            print "Output key %s = " %k

# Not sure if we need to allow for a more generic executor function or just automatically call striped_executor above.
def run_dark_higgs():
    session_name = "striped_130tb.yaml"
    dataset = ["NanoTuples-2016_QCD_HT1500to2000_TuneCUETP8M1_13TeV-madgraphMLM-pythia8"]
    # XX instantiate proc_instance
    exec_function(dataset, session_name, proc_instance)

