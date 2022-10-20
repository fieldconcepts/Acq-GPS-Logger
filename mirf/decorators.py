import numpy as np
import datetime

def AbsMaxMagnitudeCluster(f):
    '''
    Decorator function that takes a MirfFileCluster class and 
    returns an array of maximum absolute magnitudes in each file.
    Uses "absmaxmag" decorator attribute name.
    '''
    orig_add = f.add_file
    orig_init = f.__init__
    orig_reset = f.reset_data
    
    def __init__(self, *args, **kwargs):
        orig_init(self, *args, **kwargs)
        self.decorators.append("absmaxmag")
        self.decorators.append("absmaxmagtime")
        self.absmaxmag = np.array([], dtype=np.float32)
        self.absmaxmagtime = np.array([], dtype=np.datetime64)
        
    def add_file(self, *args, **kwargs):
        m = args[0]
        data = m.get_all_data()
        chn = 0

        # get max abs mag
        self.absmaxmag = np.append(self.absmaxmag, np.amax(np.abs(data[:,chn])))

        # get index of max and covnert to datetime
        absmaxmag_idx = np.argmax(np.abs(data[:,chn]))
        absmaxmag_t = datetime.timedelta(microseconds=int(m.sample_period * absmaxmag_idx)) + m.time
        self.absmaxmagtime = np.append(self.absmaxmagtime, absmaxmag_t)

        orig_add(self, *args, **kwargs)
    
    def reset_data(self):    
        self.absmaxmag = np.array([], dtype=np.float32)
        self.absmaxmagtime = np.array([], dtype=np.datetime64)
        orig_reset(self)
    f.__init__ = __init__
    f.add_file = add_file
    f.reset_data = reset_data
    return f

def MaxMagnitudeCluster(f):
    '''
    Decorator function that takes a MirfFileCluster class and 
    returns arrays of maximum magnitude and corresponding datetime in each file.
    Uses "max mags" and "maxmagtime" decorator attribute names.
    '''
    orig_add = f.add_file
    orig_init = f.__init__
    orig_reset = f.reset_data
    
    def __init__(self, *args, **kwargs):
        orig_init(self, *args, **kwargs)

        # decorator names must match class
        self.decorators.append("maxmag")
        self.decorators.append("maxmagtime")
        
        self.maxmag = np.array([], dtype=np.float32)
        self.maxmagtime = np.array([], dtype=np.datetime64)
        
    def add_file(self, *args, **kwargs):
        m = args[0]
        data = m.get_all_data()
        chn = 0


        # get max value in file
        self.maxmag = np.append(self.maxmag, np.amax(data[:,chn]))

        # get index of max and covnert to datetime
        maxmag_idx = np.argmax(data[:,chn])
        maxmag_t = datetime.timedelta(microseconds=int(m.sample_period * maxmag_idx)) + m.time
        self.maxmagtime = np.append(self.maxmagtime, maxmag_t)

        orig_add(self, *args, **kwargs)
    
    def reset_data(self):    
        self.maxmag = np.array([], dtype=np.float32)
        self.maxmagtime = np.array([], dtype=np.datetime64)
        orig_reset(self)

    f.__init__ = __init__
    f.add_file = add_file
    f.reset_data = reset_data
    return f

def MinMagnitudeCluster(f):
    '''
    Decorator function that takes a MirfFileCluster class and 
    returns minimum magitude and corresponding datetime in each file.
    Uses "minmags" and "minmagtime" decorator attribute names.
    '''
    orig_add = f.add_file
    orig_init = f.__init__
    orig_reset = f.reset_data
    
    def __init__(self, *args, **kwargs):
        orig_init(self, *args, **kwargs)

        # decorator names must match class
        self.decorators.append("minmag")
        self.decorators.append("minmagtime")
        
        self.minmag = np.array([], dtype=np.float32)
        self.minmagtime = np.array([], dtype=np.datetime64)
        
    def add_file(self, *args, **kwargs):
        m = args[0]
        data = m.get_all_data()
        chn = 0

        # get max value in file
        self.minmag = np.append(self.minmag, np.amin(data[:,chn]))

        # get index of max and covnert to datetime
        minmag_idx = np.argmin(data[:,chn])
        minmag_t = datetime.timedelta(microseconds=int(m.sample_period * minmag_idx)) + m.time
        self.minmagtime = np.append(self.minmagtime, minmag_t)

        orig_add(self, *args, **kwargs)
    
    def reset_data(self):    
        self.minmag = np.array([], dtype=np.float32)
        self.minmagtime = np.array([], dtype=np.datetime64)
        orig_reset(self)

    f.__init__ = __init__
    f.add_file = add_file
    f.reset_data = reset_data
    return f

def EnergyCluster(f):
    '''
    Decorator function that takes a MirfFileCluster class and 
    returns an array of energy in each file.
    Uses "energy" decorator attribute name.
    '''
    orig_add = f.add_file
    orig_init = f.__init__
    orig_reset = f.reset_data
    
    def __init__(self, *args, **kwargs):
        orig_init(self, *args, **kwargs)
        self.decorators.append("energy")
        self.energy = np.array([], dtype=np.float32)
        
    def add_file(self, *args, **kwargs):
        m = args[0]
        data = m.get_all_data()
        chn = 0

        self.energy = np.append(self.energy, np.sum(np.square(data[:,chn])))
        orig_add(self, *args, **kwargs)
    
    def reset_data(self):    
        self.energy = np.array([], dtype=np.float32)
        orig_reset(self)
    f.__init__ = __init__
    f.add_file = add_file
    f.reset_data = reset_data
    return f

