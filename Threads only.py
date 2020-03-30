import threading
import time

class populateUDM (threading.Thread):
    progress = dict ()

    def __init__(self, name, view, table, sleeptime):
        threading.Thread.__init__(self) # Init the super class
        self.name = name
        self.view = view
        self.table = table
        self.sleeptime = sleeptime
      
    def run (self): # Upon thread.start this function executes
        print (self.name + ": starting thread")
        
        threadLock.acquire ()
        populateUDM.progress [self.name] = "started"
        threadLock.release ()

        # Populate UDM table from view
        # INSERT INTO self.table SELECT * FROM self.view
        print (self.name + ": processing UDM table " + self.table)
        time.sleep (self.sleeptime)
        print (self.name + ": processed UDM table " + self.table)

        threadLock.acquire ()
        populateUDM.progress [self.name] = 'complete'
        threadLock.release ()
        
        print (self.name + ": exiting thread normally")

        # Exception:
        # populateUDM.progress [self.name] = 'Error!"

thread1 = populateUDM ("Organisation name", "v_udm_organisation_name", "udm_organisation_name", 2)
thread2 = populateUDM ("Party reference", "v_udm_party_reference", "udm_party_reference", 5)

# Create lock
threadLock = threading.Lock()
threads = [] # List of threads
threads.append (thread1)
threads.append (thread2)

# Start new Threads
thread1.start ()
time.sleep (0.0001) # Otherwise starting thread messages get intermingled
thread2.start ()

for thread in threads: # Wait for threads before main exit
    thread.join ()
print ("Result report")
print (populateUDM.progress)

time.sleep (5)
