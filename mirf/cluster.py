import glob
import mirf
import os.path
import datetime

import numpy as np
import re
import hashlib
import pickle

class MirfFileCluster:
    
    file_expr = re.compile(r'(?:m|f)_(\d+)\.rcd')
    
    def __init__(self, folder = None):
        self.reset_data()
        
        self.grads = 20
        self.folders = []
        self.decorators = []
        
        if(folder):
            self.add_folder(folder)
            self._parse_folder(folder)
            
    def add_folder(self, folder):
        self.folders.append(folder)
        
    def parse(self):
        objs = [] # We will hold all folders
        filelist = []
        for folder in self.folders:
            f = self._parse_folder(folder)
            with open(f,'rb') as fh:
                obj = pickle.load(fh)
                
            objs.append(obj)
            filelist += [os.path.join(folder,fh) for fh in obj.filelist]
            
        # We now have all the objects, we will create our MirfCluster class data
        self.reset_data()
        for obj in objs:
            self.timestamps = np.append(self.timestamps, obj.timestamps)
            self.durations = np.append(self.durations, obj.durations)
            for att in obj.decorators:
                setattr(self,att, np.append(getattr(self, att),getattr(obj,att)))
                
        self.filelist = filelist
        
    def _parse_folder(self, folder, show_progress= True):
        
        #self.filelist = [f for f in glob.glob(os.path.join(folder,"*.rcd"))]
        files = glob.glob(os.path.join(folder,"*.rcd"))
        files.sort()
        
        h = hashlib.md5()
        h.update(":".join(self.decorators).encode())
        h.update("::".encode())
        h.update(";".join([os.path.basename(f) for f in files]).encode())
        cache = os.path.join(folder,h.hexdigest()+".idx")
        
        if(os.path.exists(cache)):
            # We have already parsed this folder with assigned decorators before
            return cache
        
        self.reset_data()
        for ii,f in enumerate(files):
            m = mirf.MirfFile(f)
            self.add_file(m)
            self.filelist.append(os.path.basename(f))
            
            if show_progress and len(files) > 100 and not ii % (len(files)//100):
                print("Processed {:.0f}% of files in cluster".format(ii/len(files)*100))
                
        with open(cache,'wb') as f:
            pickle.dump(self,f)
        return cache         
        
                
    def reset_data(self):
        self.filelist = []
        self.timestamps = np.array([], dtype=np.datetime64)
        self.durations = np.array([], dtype=np.timedelta64)
        
    def add_file(self, m):

        t = m.time
        N = m.channels[0].N
        N = N - m.overlap_samples
        self.timestamps = np.append(self.timestamps, t)
        self.durations = np.append(self.durations, datetime.timedelta(microseconds=N*m.sample_period))
        
    def __iter__(self):
        self.ptr = 0
        return self # We are an iterator class
    
    def __next__(self):
        
        nofiles = len(self.filelist)
        
        if (self.ptr >= nofiles):
            raise StopIteration
        
        ptr = self.ptr
        self.ptr += 1
        
        return self.filelist[ptr]
    
    def __len__(self):
        return len(self.filelist)
    
    def locate(self, t):
        
        if isinstance(t, str):
            t = datetime.datetime.fromisoformat(t)
            
        if isinstance(t, datetime.datetime):
            if t.tzinfo == None:
                t = t.replace(tzinfo=datetime.timezone.utc)
            
            for ii,filetime in enumerate(self.timestamps):
                if(t >= filetime and t < (filetime + self.durations[ii])):
                    return self.filelist[ii]
                
                if(t < filetime): raise IndexError("Timestamp not found in cluster!")
            
            raise IndexError("Timestamp not found in cluster!")
        raise ValueError("A timestamp must be provided")
        
    def get_rcd_no(filename) -> int:
        r = MirfFileCluster.file_expr.search(filename)
        
        # In the RegExp search this is one group saved, which is the rcd number.
        return int(r.groups()[0])
        
        
    def get_data_between(self, t0: datetime.datetime, tf: datetime.datetime):
        
        # Check whether a string has been passed representing a datetime.
        if isinstance(t0, str): t0 = datetime.datetime.fromisoformat(t0)
        if isinstance(tf, str): tf = datetime.datetime.fromisoformat(tf)
        
        # Now check that all inputs are datetimes
        if isinstance(t0, datetime.datetime) and isinstance(tf, datetime.datetime):
            
            # UTC is implied if not directly specified
            if t0.tzinfo == None: t0 = t0.replace(tzinfo=datetime.timezone.utc)
            if tf.tzinfo == None: tf = tf.replace(tzinfo=datetime.timezone.utc)
            
            # Check that the finish time is not before the starting time.
            if tf < t0: raise ValueError("End time is before starting time.")
            
            # We will locate the relevant MIRF file, get its RCD number, and open it.
            first_file = self.locate(t0)
            file_no = MirfFileCluster.get_rcd_no(first_file)
            f = mirf.MirfFile(first_file)
            
            # Now we get the file's finish time and find the start and end sample offsets.
            t_end = f.get_finish_timestamp()
            tp = tf if t_end > tf else t_end
            n0 = f.get_sample_offset(t0)
            nf = f.get_sample_offset(tp)
            
            data = f.get_all_data()[round(n0):round(nf),:]
            
            while(t_end < tf):

                t0 = tp
                file = self.locate(t0)
                fp = mirf.MirfFile(file)
                
                t_end = fp.get_finish_timestamp()
                tp = tf if t_end > tf else t_end
                n0 = fp.get_sample_offset(t0) # Should always be zero
                nf = fp.get_sample_offset(tp)

                data = np.append(data, fp.get_all_data()[round(n0):round(nf),:],axis=0)                
            
            return data
            
        raise ValueError("get_data_between requires two datetime arguments.")
        
    def get_datetime_T0(self, t0: datetime.datetime):
        '''
        Function that takes a start time in the form of a datetime obejct and returns
        the first valid datetime object within the dataset..
        
        '''
    
        # Check whether a string has been passed representing a datetime.
        if isinstance(t0, str): t0 = datetime.datetime.fromisoformat(t0)
       
        # Now check that all inputs are datetimes
        if isinstance(t0, datetime.datetime):
            
            # UTC is implied if not directly specified
            if t0.tzinfo == None: t0 = t0.replace(tzinfo=datetime.timezone.utc)

            # We will locate the relevant MIRF file, get its RCD number, and open it.
            first_file = self.locate(t0)
            f = mirf.MirfFile(first_file)
            
            # Now we get the file's start time and find the start sample offset.
            n0 = f.get_sample_offset(t0)

            # convert offset to first sample datetime
            time_zero = f.time + datetime.timedelta(microseconds=320 * round(n0))

            return time_zero
            
        raise ValueError("get_data_between_T0 requires a valid start datetime.")